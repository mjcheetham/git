#define DISABLE_SIGN_COMPARE_WARNINGS

#include "git-compat-util.h"
#include "git-curl-compat.h"

#include <curl/curl.h>
#include <curl/easy.h>

#include "http-client.h"
#include "http-common.h"
#include "config.h"
#include "credential.h"
#include "environment.h"
#include "gettext.h"
#include "pkt-line.h"
#include "strvec.h"
#include "trace.h"
#include "transport.h"
#include "url.h"
#include "urlmatch.h"
#include "version.h"

/* ================================================================== */
/*  Client context (full definition — opaque to consumers)             */
/* ================================================================== */

struct http_client {
	struct http_client_options opts;
	struct repository *repo;
	CURLM *multi;
	CURL *curl_template;

	struct credential auth;
	struct credential proxy_auth;
	struct credential cert_auth;
	struct credential proxy_cert_auth;

	unsigned long auth_methods;
	int auth_methods_restricted;

	char *accept_language;

	struct curl_slist *default_headers;
	struct curl_slist *host_resolutions;

	int active_requests;
	int session_count;
};

/* ================================================================== */
/*  Tracing helpers                                                    */
/* ================================================================== */

static struct trace_key trace_curl = TRACE_KEY_INIT(CURL);

static int redact_sensitive_header(struct strbuf *header, size_t offset)
{
	const char *sensitive_header;

	if (skip_iprefix(header->buf + offset, "Authorization:", &sensitive_header) ||
	    skip_iprefix(header->buf + offset, "Proxy-Authorization:", &sensitive_header)) {
		while (isspace(*sensitive_header))
			sensitive_header++;
		while (*sensitive_header && !isspace(*sensitive_header))
			sensitive_header++;
		strbuf_setlen(header, sensitive_header - header->buf);
		strbuf_addstr(header, " <redacted>");
		return 1;
	}

	if (skip_iprefix(header->buf + offset, "Cookie:", &sensitive_header)) {
		struct strbuf redacted = STRBUF_INIT;
		const char *cookie;

		while (isspace(*sensitive_header))
			sensitive_header++;
		cookie = sensitive_header;

		while (cookie) {
			const char *equals;
			const char *semicolon = strstr(cookie, "; ");
			const char *end = semicolon ? semicolon : cookie + strlen(cookie);

			equals = memchr(cookie, '=', end - cookie);
			if (!equals) {
				strbuf_add(&redacted, cookie, end - cookie);
			} else {
				strbuf_add(&redacted, cookie, equals - cookie);
				strbuf_addstr(&redacted, "=<redacted>");
			}
			if (semicolon) {
				strbuf_addstr(&redacted, "; ");
				cookie = semicolon + strlen("; ");
			} else {
				cookie = NULL;
			}
		}

		strbuf_setlen(header, sensitive_header - header->buf);
		strbuf_addbuf(header, &redacted);
		strbuf_release(&redacted);
		return 1;
	}

	return 0;
}

static int match_curl_h2_trace(const char *line, const char **out)
{
	const char *p;

	if (skip_iprefix(line, "h2h3 [", out) ||
	    skip_iprefix(line, "h2 [", out))
		return 1;

	if (skip_iprefix(line, "[HTTP/2] [", &p)) {
		while (isdigit(*p))
			p++;
		if (skip_prefix(p, "] [", out))
			return 1;
	}
	return 0;
}

static void redact_sensitive_info_header(struct strbuf *header)
{
	const char *sensitive_header;

	if (match_curl_h2_trace(header->buf, &sensitive_header)) {
		if (redact_sensitive_header(header, sensitive_header - header->buf))
			strbuf_addch(header, ']');
	}
}

static void dump_header(const char *text, unsigned char *ptr, size_t size,
			int hide_sensitive)
{
	struct strbuf out = STRBUF_INIT;
	struct strbuf **headers, **h;

	strbuf_addf(&out, "%s, %10.10ld bytes (0x%8.8lx)\n",
		    text, (long)size, (long)size);
	trace_strbuf(&trace_curl, &out);
	strbuf_reset(&out);
	strbuf_add(&out, ptr, size);
	headers = strbuf_split_max(&out, '\n', 0);

	for (h = headers; *h; h++) {
		if (hide_sensitive)
			redact_sensitive_header(*h, 0);
		strbuf_insertstr(*h, 0, text);
		strbuf_insertstr(*h, strlen(text), ": ");
		strbuf_rtrim(*h);
		strbuf_addch(*h, '\n');
		trace_strbuf(&trace_curl, *h);
	}
	strbuf_list_free(headers);
	strbuf_release(&out);
}

static void dump_data(const char *text, unsigned char *ptr, size_t size)
{
	size_t i;
	struct strbuf out = STRBUF_INIT;
	unsigned int width = 60;

	strbuf_addf(&out, "%s, %10.10ld bytes (0x%8.8lx)\n",
		    text, (long)size, (long)size);
	trace_strbuf(&trace_curl, &out);

	for (i = 0; i < size; i += width) {
		size_t w;
		strbuf_reset(&out);
		strbuf_addf(&out, "%s: ", text);
		for (w = 0; w < width && i + w < size; w++) {
			unsigned char ch = ptr[i + w];
			strbuf_addch(&out, (ch >= 0x20 && ch < 0x80) ? ch : '.');
		}
		strbuf_addch(&out, '\n');
		trace_strbuf(&trace_curl, &out);
	}
	strbuf_release(&out);
}

static void dump_info(char *data, size_t size)
{
	struct strbuf buf = STRBUF_INIT;
	strbuf_add(&buf, data, size);
	redact_sensitive_info_header(&buf);
	trace_printf_key(&trace_curl, "== Info: %s", buf.buf);
	strbuf_release(&buf);
}

struct trace_context {
	int trace_data;
	int trace_redact;
};

static int curl_trace_cb(CURL *handle UNUSED, curl_infotype type,
			 char *data, size_t size, void *userp)
{
	struct trace_context *ctx = userp;
	enum { NO_FILTER = 0, DO_FILTER = 1 };

	switch (type) {
	case CURLINFO_TEXT:
		dump_info(data, size);
		break;
	case CURLINFO_HEADER_OUT:
		dump_header("=> Send header", (unsigned char *)data, size, DO_FILTER);
		break;
	case CURLINFO_DATA_OUT:
		if (ctx->trace_data)
			dump_data("=> Send data", (unsigned char *)data, size);
		break;
	case CURLINFO_SSL_DATA_OUT:
		if (ctx->trace_data)
			dump_data("=> Send SSL data", (unsigned char *)data, size);
		break;
	case CURLINFO_HEADER_IN:
		dump_header("<= Recv header", (unsigned char *)data, size, NO_FILTER);
		break;
	case CURLINFO_DATA_IN:
		if (ctx->trace_data)
			dump_data("<= Recv data", (unsigned char *)data, size);
		break;
	case CURLINFO_SSL_DATA_IN:
		if (ctx->trace_data)
			dump_data("<= Recv SSL data", (unsigned char *)data, size);
		break;
	default:
		return 0;
	}
	return 0;
}

static void setup_curl_tracing(CURL *handle, struct trace_context *tctx)
{
	if (!trace_want(&trace_curl))
		return;
	curl_easy_setopt(handle, CURLOPT_VERBOSE, 1L);
	curl_easy_setopt(handle, CURLOPT_DEBUGFUNCTION, curl_trace_cb);
	curl_easy_setopt(handle, CURLOPT_DEBUGDATA, tctx);
}

/* ================================================================== */
/*  Accept-Language                                                    */
/* ================================================================== */

static void build_accept_language(struct strbuf *buf)
{
	const int MAX_DECIMAL_PLACES = 3;
	const int MAX_LANGUAGE_TAGS = 1000;
	const int MAX_HEADER_SIZE = 4000;
	char **tags = NULL;
	int num = 0;
	const char *s = get_preferred_languages();
	int i;
	struct strbuf tag = STRBUF_INIT;

	if (!s)
		return;

	do {
		for (; *s && (isalnum(*s) || *s == '_'); s++)
			strbuf_addch(&tag, *s == '_' ? '-' : *s);
		while (*s && *s != ':')
			s++;

		if (tag.len) {
			num++;
			REALLOC_ARRAY(tags, num);
			tags[num - 1] = strbuf_detach(&tag, NULL);
			if (num >= MAX_LANGUAGE_TAGS - 1)
				break;
		}
	} while (*s++);

	if (num) {
		int last_len = 0, max_q, decimal_places;
		char q_format[32];

		REALLOC_ARRAY(tags, num + 1);
		tags[num++] = xstrdup("*");

		for (max_q = 1, decimal_places = 0;
		     max_q < num && decimal_places <= MAX_DECIMAL_PLACES;
		     decimal_places++, max_q *= 10)
			;

		xsnprintf(q_format, sizeof(q_format),
			  ";q=0.%%0%dd", decimal_places);

		strbuf_addstr(buf, "Accept-Language: ");
		for (i = 0; i < num; i++) {
			if (i > 0)
				strbuf_addstr(buf, ", ");
			strbuf_addstr(buf, tags[i]);
			if (i > 0)
				strbuf_addf(buf, q_format, max_q - i);
			if (buf->len > MAX_HEADER_SIZE) {
				strbuf_remove(buf, last_len,
					      buf->len - last_len);
				break;
			}
			last_len = buf->len;
		}
	}

	for (i = 0; i < num; i++)
		free(tags[i]);
	free(tags);
}

const char *http_client_accept_language(struct http_client *client)
{
	if (!client->accept_language) {
		struct strbuf buf = STRBUF_INIT;
		build_accept_language(&buf);
		if (buf.len > 0)
			client->accept_language = strbuf_detach(&buf, NULL);
		else
			strbuf_release(&buf);
	}
	return client->accept_language;
}

/* ================================================================== */
/*  Content-type extraction                                            */
/* ================================================================== */

static int extract_param(const char *raw, const char *name,
			 struct strbuf *out)
{
	size_t len = strlen(name);

	if (strncasecmp(raw, name, len))
		return -1;
	raw += len;
	if (*raw != '=')
		return -1;
	raw++;

	while (*raw && !isspace(*raw) && *raw != ';')
		strbuf_addch(out, *raw++);
	return 0;
}

static void extract_content_type(struct strbuf *raw, struct strbuf *type,
				 struct strbuf *charset)
{
	const char *p;

	strbuf_reset(type);
	strbuf_grow(type, raw->len);
	for (p = raw->buf; *p; p++) {
		if (isspace(*p))
			continue;
		if (*p == ';') {
			p++;
			break;
		}
		strbuf_addch(type, tolower(*p));
	}

	if (!charset)
		return;

	strbuf_reset(charset);
	while (*p) {
		while (isspace(*p) || *p == ';')
			p++;
		if (!extract_param(p, "charset", charset))
			return;
		while (*p && !isspace(*p))
			p++;
	}

	if (!charset->len && starts_with(type->buf, "text/"))
		strbuf_addstr(charset, "ISO-8859-1");
}

/* ================================================================== */
/*  Configuration loading                                              */
/* ================================================================== */

struct config_load_context {
	struct http_client_options *opts;
};

static int http_client_config_cb(const char *var, const char *value,
				 const struct config_context *ctx, void *data)
{
	struct config_load_context *load = data;
	struct http_client_options *opts = load->opts;

	/* SSL/TLS */
	if (!strcmp("http.sslverify", var)) {
		opts->ssl.ssl_verify = git_config_bool(var, value);
		return 0;
	}
	if (!strcmp("http.sslcipherlist", var))
		return git_config_string(&opts->ssl.ssl_cipher_list, var, value);
	if (!strcmp("http.sslversion", var))
		return git_config_string(&opts->ssl.ssl_version, var, value);
	if (!strcmp("http.sslcert", var))
		return git_config_pathname(&opts->ssl.ssl_cert, var, value);
	if (!strcmp("http.sslcerttype", var))
		return git_config_string(&opts->ssl.ssl_cert_type, var, value);
	if (!strcmp("http.sslkey", var))
		return git_config_pathname(&opts->ssl.ssl_key, var, value);
	if (!strcmp("http.sslkeytype", var))
		return git_config_string(&opts->ssl.ssl_key_type, var, value);
	if (!strcmp("http.sslcapath", var))
		return git_config_pathname(&opts->ssl.ssl_ca_path, var, value);
	if (!strcmp("http.sslcainfo", var))
		return git_config_pathname(&opts->ssl.ssl_ca_info, var, value);
	if (!strcmp("http.sslcertpasswordprotected", var)) {
		opts->ssl.cert_password_required = git_config_bool(var, value);
		return 0;
	}
	if (!strcmp("http.ssltry", var)) {
		opts->ssl.ssl_try = git_config_bool(var, value);
		return 0;
	}
	if (!strcmp("http.sslbackend", var)) {
		free(opts->ssl.ssl_backend);
		opts->ssl.ssl_backend = xstrdup_or_null(value);
		return 0;
	}
	if (!strcmp("http.schannelcheckrevoke", var)) {
		opts->ssl.schannel_check_revoke = git_config_bool(var, value);
		return 0;
	}
	if (!strcmp("http.schannelusesslcainfo", var)) {
		opts->ssl.schannel_use_ssl_cainfo = git_config_bool(var, value);
		return 0;
	}
	if (!strcmp("http.pinnedpubkey", var))
		return git_config_pathname(&opts->ssl.ssl_pinned_key, var, value);

	/* Protocol / version */
	if (!strcmp("http.version", var))
		return git_config_string(&opts->http_version, var, value);

	/* Connection limits */
	if (!strcmp("http.maxrequests", var)) {
		opts->max_requests = git_config_int(var, value, ctx->kvi);
		return 0;
	}

	/* Timeouts */
	if (!strcmp("http.lowspeedlimit", var)) {
		opts->low_speed_limit = git_config_int(var, value, ctx->kvi);
		return 0;
	}
	if (!strcmp("http.lowspeedtime", var)) {
		opts->low_speed_time = git_config_int(var, value, ctx->kvi);
		return 0;
	}
	if (!strcmp("http.keepaliveidle", var)) {
		opts->tcp_keepidle = git_config_int(var, value, ctx->kvi);
		return 0;
	}
	if (!strcmp("http.keepaliveinterval", var)) {
		opts->tcp_keepintvl = git_config_int(var, value, ctx->kvi);
		return 0;
	}
	if (!strcmp("http.keepalivecount", var)) {
		opts->tcp_keepcnt = git_config_int(var, value, ctx->kvi);
		return 0;
	}

	/* Proxy */
	if (!strcmp("http.proxy", var))
		return git_config_string(&opts->proxy.proxy_url, var, value);
	if (!strcmp("http.proxyauthmethod", var))
		return git_config_string(&opts->proxy.proxy_auth_method, var, value);
	if (!strcmp("http.proxysslcert", var))
		return git_config_string(&opts->proxy.proxy_ssl_cert, var, value);
	if (!strcmp("http.proxysslkey", var))
		return git_config_string(&opts->proxy.proxy_ssl_key, var, value);
	if (!strcmp("http.proxysslcainfo", var))
		return git_config_string(&opts->proxy.proxy_ssl_ca_info, var, value);
	if (!strcmp("http.proxysslcertpasswordprotected", var)) {
		opts->proxy.proxy_ssl_cert_password_required =
			git_config_bool(var, value);
		return 0;
	}

	/* Cookies */
	if (!strcmp("http.cookiefile", var))
		return git_config_pathname(&opts->cookie_file, var, value);
	if (!strcmp("http.savecookies", var)) {
		opts->save_cookies = git_config_bool(var, value);
		return 0;
	}

	/* Transfer */
	if (!strcmp("http.postbuffer", var)) {
		opts->post_buffer = git_config_ssize_t(var, value, ctx->kvi);
		if (opts->post_buffer < 0)
			warning(_("negative value for http.postBuffer; "
				  "defaulting to %d"), LARGE_PACKET_MAX);
		if (opts->post_buffer < LARGE_PACKET_MAX)
			opts->post_buffer = LARGE_PACKET_MAX;
		return 0;
	}
	if (!strcmp("http.useragent", var))
		return git_config_string(&opts->user_agent, var, value);

	/* Auth */
	if (!strcmp("http.emptyauth", var)) {
		if (value && !strcmp("auto", value))
			opts->empty_auth = -1;
		else
			opts->empty_auth = git_config_bool(var, value);
		return 0;
	}
	if (!strcmp("http.proactiveauth", var)) {
		if (!value)
			return config_error_nonbool(var);
		if (!strcmp(value, "auto"))
			opts->proactive_auth = HTTP_CLIENT_AUTH_AUTO;
		else if (!strcmp(value, "basic"))
			opts->proactive_auth = HTTP_CLIENT_AUTH_BASIC;
		else if (!strcmp(value, "none"))
			opts->proactive_auth = HTTP_CLIENT_AUTH_NONE;
		else
			warning(_("Unknown value for http.proactiveauth"));
		return 0;
	}

	/* Extra headers */
	if (!strcmp("http.extraheader", var)) {
		if (!value)
			return config_error_nonbool(var);
		else if (!*value)
			string_list_clear(&opts->extra_headers, 0);
		else
			string_list_append(&opts->extra_headers, value);
		return 0;
	}

	/* Host resolutions */
	if (!strcmp("http.curloptresolve", var)) {
		if (!value)
			return config_error_nonbool(var);
		else if (!*value)
			string_list_clear(&opts->host_resolutions, 0);
		else
			string_list_append(&opts->host_resolutions, value);
		return 0;
	}

	/* Redirect policy */
	if (!strcmp("http.followredirects", var)) {
		if (value && !strcmp(value, "initial"))
			opts->follow = HTTP_CLIENT_FOLLOW_INITIAL;
		else if (git_config_bool(var, value))
			opts->follow = HTTP_CLIENT_FOLLOW_ALWAYS;
		else
			opts->follow = HTTP_CLIENT_FOLLOW_NONE;
		return 0;
	}

	/* FTP */
	if (!strcmp("http.noepsv", var)) {
		opts->ftp_no_epsv = git_config_bool(var, value);
		return 0;
	}

	return git_default_config(var, value, ctx, data);
}

static void set_from_env(char **var, const char *envname)
{
	const char *val = getenv(envname);
	if (val) {
		FREE_AND_NULL(*var);
		*var = xstrdup(val);
	}
}

static void set_long_from_env(long *var, const char *envname)
{
	const char *val = getenv(envname);
	if (val) {
		long tmp;
		char *endp;
		int saved_errno = errno;
		errno = 0;
		tmp = strtol(val, &endp, 10);
		if (errno)
			warning_errno(_("failed to parse %s"), envname);
		else if (*endp || endp == val)
			warning(_("failed to parse %s"), envname);
		else
			*var = tmp;
		errno = saved_errno;
	}
}

void http_client_options_load(struct http_client_options *opts,
			      struct repository *repo,
			      const char *url)
{
	char *normalized_url;
	struct urlmatch_config config = URLMATCH_CONFIG_INIT;

	config.section = "http";
	config.key = NULL;
	config.collect_fn = http_client_config_cb;
	config.cascade_fn = git_default_config;
	config.cb = &(struct config_load_context){ .opts = opts };

	normalized_url = url_normalize(url, &config.url);
	repo_config(repo, urlmatch_config_entry, &config);
	free(normalized_url);
	string_list_clear(&config.vars, 1);

	/* Environment overrides */
	if (getenv("GIT_SSL_NO_VERIFY"))
		opts->ssl.ssl_verify = 0;
	set_from_env(&opts->ssl.ssl_cert, "GIT_SSL_CERT");
	set_from_env(&opts->ssl.ssl_cert_type, "GIT_SSL_CERT_TYPE");
	set_from_env(&opts->ssl.ssl_key, "GIT_SSL_KEY");
	set_from_env(&opts->ssl.ssl_key_type, "GIT_SSL_KEY_TYPE");
	set_from_env(&opts->ssl.ssl_ca_path, "GIT_SSL_CAPATH");
	set_from_env(&opts->ssl.ssl_ca_info, "GIT_SSL_CAINFO");
	set_from_env(&opts->user_agent, "GIT_HTTP_USER_AGENT");

	set_long_from_env(&opts->low_speed_limit, "GIT_HTTP_LOW_SPEED_LIMIT");
	set_long_from_env(&opts->low_speed_time, "GIT_HTTP_LOW_SPEED_TIME");

	set_from_env(&opts->proxy.proxy_ssl_cert, "GIT_PROXY_SSL_CERT");
	set_from_env(&opts->proxy.proxy_ssl_key, "GIT_PROXY_SSL_KEY");
	set_from_env(&opts->proxy.proxy_ssl_ca_info, "GIT_PROXY_SSL_CAINFO");

	if (getenv("GIT_PROXY_SSL_CERT_PASSWORD_PROTECTED"))
		opts->proxy.proxy_ssl_cert_password_required = 1;
	if (getenv("GIT_CURL_FTP_NO_EPSV"))
		opts->ftp_no_epsv = 1;

	set_long_from_env(&opts->tcp_keepidle, "GIT_TCP_KEEPIDLE");
	set_long_from_env(&opts->tcp_keepintvl, "GIT_TCP_KEEPINTVL");
	set_long_from_env(&opts->tcp_keepcnt, "GIT_TCP_KEEPCNT");

	{
		char *http_max = getenv("GIT_HTTP_MAX_REQUESTS");
		if (http_max)
			opts->max_requests = atoi(http_max);
	}

	if (opts->ssl.ssl_verify == -1)
		opts->ssl.ssl_verify = 1;

	if (url && !opts->ssl.cert_password_required &&
	    getenv("GIT_SSL_CERT_PASSWORD_PROTECTED") &&
	    starts_with(url, "https://"))
		opts->ssl.cert_password_required = 1;
}

void http_client_options_release(struct http_client_options *opts)
{
	free(opts->http_version);
	free(opts->user_agent);
	free(opts->cookie_file);

	free(opts->ssl.ssl_cert);
	free(opts->ssl.ssl_cert_type);
	free(opts->ssl.ssl_key);
	free(opts->ssl.ssl_key_type);
	free(opts->ssl.ssl_ca_path);
	free(opts->ssl.ssl_ca_info);
	free(opts->ssl.ssl_cipher_list);
	free(opts->ssl.ssl_version);
	free(opts->ssl.ssl_pinned_key);
	free(opts->ssl.ssl_backend);

	free(opts->proxy.proxy_url);
	free(opts->proxy.proxy_auth_method);
	free(opts->proxy.no_proxy);
	free(opts->proxy.proxy_ssl_cert);
	free(opts->proxy.proxy_ssl_key);
	free(opts->proxy.proxy_ssl_ca_info);

	string_list_clear(&opts->extra_headers, 0);
	string_list_clear(&opts->host_resolutions, 0);

	memset(opts, 0, sizeof(*opts));
}

/* ================================================================== */
/*  Allowed protocols                                                  */
/* ================================================================== */

static void proto_list_append(struct strbuf *list, const char *proto)
{
	if (!list)
		return;
	if (list->len)
		strbuf_addch(list, ',');
	strbuf_addstr(list, proto);
}

static long get_allowed_protocols(int from_user, struct strbuf *list)
{
	long bits = 0;

	if (is_transport_allowed("http", from_user)) {
		bits |= CURLPROTO_HTTP;
		proto_list_append(list, "http");
	}
	if (is_transport_allowed("https", from_user)) {
		bits |= CURLPROTO_HTTPS;
		proto_list_append(list, "https");
	}
	if (is_transport_allowed("ftp", from_user)) {
		bits |= CURLPROTO_FTP;
		proto_list_append(list, "ftp");
	}
	if (is_transport_allowed("ftps", from_user)) {
		bits |= CURLPROTO_FTPS;
		proto_list_append(list, "ftps");
	}
	return bits;
}

/* ================================================================== */
/*  SSL version mapping                                                */
/* ================================================================== */

static struct {
	const char *name;
	long ssl_version;
} sslversions[] = {
	{ "sslv2", CURL_SSLVERSION_SSLv2 },
	{ "sslv3", CURL_SSLVERSION_SSLv3 },
	{ "tlsv1", CURL_SSLVERSION_TLSv1 },
	{ "tlsv1.0", CURL_SSLVERSION_TLSv1_0 },
	{ "tlsv1.1", CURL_SSLVERSION_TLSv1_1 },
	{ "tlsv1.2", CURL_SSLVERSION_TLSv1_2 },
	{ "tlsv1.3", CURL_SSLVERSION_TLSv1_3 },
};

static struct {
	const char *name;
	long curlauth_param;
} proxy_authmethods[] = {
	{ "basic", CURLAUTH_BASIC },
	{ "digest", CURLAUTH_DIGEST },
	{ "negotiate", CURLAUTH_GSSNEGOTIATE },
	{ "ntlm", CURLAUTH_NTLM },
	{ "anyauth", CURLAUTH_ANY },
};

#ifdef CURLGSSAPI_DELEGATION_FLAG
static struct {
	const char *name;
	long curl_deleg_param;
} curl_deleg_levels[] = {
	{ "none", CURLGSSAPI_DELEGATION_NONE },
	{ "policy", CURLGSSAPI_DELEGATION_POLICY_FLAG },
	{ "always", CURLGSSAPI_DELEGATION_FLAG },
};
#endif

/* ================================================================== */
/*  HTTP version mapping                                               */
/* ================================================================== */

static int get_http_version_opt(const char *version_string, long *opt)
{
	static struct {
		const char *name;
		long opt_token;
	} choices[] = {
		{ "HTTP/1.1", CURL_HTTP_VERSION_1_1 },
		{ "HTTP/2", CURL_HTTP_VERSION_2 },
	};
	int i;

	for (i = 0; i < ARRAY_SIZE(choices); i++) {
		if (!strcmp(version_string, choices[i].name)) {
			*opt = choices[i].opt_token;
			return 0;
		}
	}
	warning("unknown value given to http.version: '%s'", version_string);
	return -1;
}

/* ================================================================== */
/*  Credential helpers                                                 */
/* ================================================================== */

static int has_cert_password(struct http_client *client)
{
	const struct http_client_ssl_options *ssl = &client->opts.ssl;

	if (!ssl->ssl_cert || !ssl->cert_password_required)
		return 0;
	if (!client->cert_auth.password) {
		client->cert_auth.protocol = xstrdup("cert");
		client->cert_auth.host = xstrdup("");
		client->cert_auth.username = xstrdup("");
		client->cert_auth.path = xstrdup(ssl->ssl_cert);
		credential_fill(client->repo, &client->cert_auth, 0);
	}
	return 1;
}

static int has_proxy_cert_password(struct http_client *client)
{
	const struct http_client_proxy_options *po = &client->opts.proxy;

	if (!po->proxy_ssl_cert || !po->proxy_ssl_cert_password_required)
		return 0;
	if (!client->proxy_cert_auth.password) {
		client->proxy_cert_auth.protocol = xstrdup("cert");
		client->proxy_cert_auth.host = xstrdup("");
		client->proxy_cert_auth.username = xstrdup("");
		client->proxy_cert_auth.path = xstrdup(po->proxy_ssl_cert);
		credential_fill(client->repo, &client->proxy_cert_auth, 0);
	}
	return 1;
}

static int always_auth_proactively(const struct http_client *client)
{
	return client->opts.proactive_auth != HTTP_CLIENT_AUTH_NONE &&
	       client->opts.proactive_auth != HTTP_CLIENT_AUTH_IF_CREDENTIALS;
}

static int empty_auth_usable(const struct http_client *client)
{
	static unsigned long useless =
		CURLAUTH_BASIC | CURLAUTH_DIGEST_IE | CURLAUTH_DIGEST;

	if (client->opts.empty_auth >= 0)
		return client->opts.empty_auth;

	if (client->auth_methods_restricted &&
	    (client->auth_methods & ~useless))
		return 1;
	return 0;
}

static struct curl_slist *append_auth_header(
		const struct credential *c,
		struct curl_slist *headers)
{
	if (c->authtype && c->credential) {
		struct strbuf auth = STRBUF_INIT;
		strbuf_addf(&auth, "Authorization: %s %s",
			    c->authtype, c->credential);
		headers = curl_slist_append(headers, auth.buf);
		strbuf_release(&auth);
	}
	return headers;
}

static void init_curl_http_auth(struct http_client *client, CURL *curl)
{
	if ((!client->auth.username || !*client->auth.username) &&
	    (!client->auth.credential || !*client->auth.credential)) {
		if (!always_auth_proactively(client) && empty_auth_usable(client)) {
			curl_easy_setopt(curl, CURLOPT_USERPWD, ":");
			return;
		} else if (!always_auth_proactively(client)) {
			return;
		} else if (client->opts.proactive_auth == HTTP_CLIENT_AUTH_BASIC) {
			strvec_push(&client->auth.wwwauth_headers, "Basic");
		}
	}

	credential_fill(client->repo, &client->auth, 1);

	if (client->auth.password) {
		if (always_auth_proactively(client))
			curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
		curl_easy_setopt(curl, CURLOPT_USERNAME, client->auth.username);
		curl_easy_setopt(curl, CURLOPT_PASSWORD, client->auth.password);
	}
}

static void init_curl_proxy_auth(struct http_client *client, CURL *curl)
{
	if (client->proxy_auth.username) {
		if (!client->proxy_auth.password &&
		    !client->proxy_auth.credential)
			credential_fill(client->repo, &client->proxy_auth, 1);

		if (client->proxy_auth.password) {
			curl_easy_setopt(curl, CURLOPT_PROXYUSERNAME,
					 client->proxy_auth.username);
			curl_easy_setopt(curl, CURLOPT_PROXYPASSWORD,
					 client->proxy_auth.password);
		} else if (client->proxy_auth.authtype &&
			   client->proxy_auth.credential) {
			curl_easy_setopt(curl, CURLOPT_PROXYHEADER,
					 append_auth_header(
						 &client->proxy_auth, NULL));
		}
	}

	if (client->opts.proxy.proxy_auth_method) {
		const char *method = client->opts.proxy.proxy_auth_method;
		int i;
		for (i = 0; i < ARRAY_SIZE(proxy_authmethods); i++) {
			if (!strcmp(method, proxy_authmethods[i].name)) {
				curl_easy_setopt(curl, CURLOPT_PROXYAUTH,
						 proxy_authmethods[i].curlauth_param);
				break;
			}
		}
		if (i == ARRAY_SIZE(proxy_authmethods)) {
			warning("unsupported proxy authentication method %s: "
				"using anyauth", method);
			curl_easy_setopt(curl, CURLOPT_PROXYAUTH, CURLAUTH_ANY);
		}
	} else {
		curl_easy_setopt(curl, CURLOPT_PROXYAUTH, CURLAUTH_ANY);
	}
}

/* ================================================================== */
/*  Building the template curl handle                                  */
/* ================================================================== */

static CURL *build_curl_template(struct http_client *client)
{
	CURL *curl = curl_easy_init();
	const struct http_client_options *opts = &client->opts;
	const struct http_client_ssl_options *ssl = &opts->ssl;
	const struct http_client_proxy_options *po = &opts->proxy;

	if (!curl)
		die("curl_easy_init failed");

	/* SSL verification */
	if (!ssl->ssl_verify) {
		curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
		curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
	} else {
		curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
		curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
	}

	/* HTTP version */
	if (opts->http_version) {
		long opt;
		if (!get_http_version_opt(opts->http_version, &opt))
			curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, opt);
	}

	curl_easy_setopt(curl, CURLOPT_NETRC, CURL_NETRC_OPTIONAL);
	curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_ANY);

#ifdef CURLGSSAPI_DELEGATION_FLAG
	/* http.delegation */
	{
		const char *deleg = getenv("GIT_HTTP_DELEGATION");
		if (deleg) {
			int i;
			for (i = 0; i < ARRAY_SIZE(curl_deleg_levels); i++) {
				if (!strcmp(deleg, curl_deleg_levels[i].name)) {
					curl_easy_setopt(curl, CURLOPT_GSSAPI_DELEGATION,
							 curl_deleg_levels[i].curl_deleg_param);
					break;
				}
			}
		}
	}
#endif

	/* Schannel revocation check */
	if (ssl->ssl_backend && !strcmp("schannel", ssl->ssl_backend) &&
	    !ssl->schannel_check_revoke)
		curl_easy_setopt(curl, CURLOPT_SSL_OPTIONS,
				 (long)CURLSSLOPT_NO_REVOKE);

	/* Proactive authentication */
	if (opts->proactive_auth != HTTP_CLIENT_AUTH_NONE)
		init_curl_http_auth(client, curl);

	/* SSL version */
	if (ssl->ssl_version && *ssl->ssl_version) {
		int i;
		for (i = 0; i < ARRAY_SIZE(sslversions); i++) {
			if (!strcmp(ssl->ssl_version, sslversions[i].name)) {
				curl_easy_setopt(curl, CURLOPT_SSLVERSION,
						 sslversions[i].ssl_version);
				break;
			}
		}
		if (i == ARRAY_SIZE(sslversions))
			warning("unsupported ssl version %s: using default",
				ssl->ssl_version);
	}

	/* SSL cipher list */
	if (ssl->ssl_cipher_list && *ssl->ssl_cipher_list)
		curl_easy_setopt(curl, CURLOPT_SSL_CIPHER_LIST,
				 ssl->ssl_cipher_list);

	/* Client certificate */
	if (ssl->ssl_cert)
		curl_easy_setopt(curl, CURLOPT_SSLCERT, ssl->ssl_cert);
	if (ssl->ssl_cert_type)
		curl_easy_setopt(curl, CURLOPT_SSLCERTTYPE, ssl->ssl_cert_type);
	if (has_cert_password(client))
		curl_easy_setopt(curl, CURLOPT_KEYPASSWD,
				 client->cert_auth.password);
	if (ssl->ssl_key)
		curl_easy_setopt(curl, CURLOPT_SSLKEY, ssl->ssl_key);
	if (ssl->ssl_key_type)
		curl_easy_setopt(curl, CURLOPT_SSLKEYTYPE, ssl->ssl_key_type);

	/* CA path / pinned key */
	if (ssl->ssl_ca_path)
		curl_easy_setopt(curl, CURLOPT_CAPATH, ssl->ssl_ca_path);
	if (ssl->ssl_pinned_key)
		curl_easy_setopt(curl, CURLOPT_PINNEDPUBLICKEY,
				 ssl->ssl_pinned_key);

	/* CA info (with schannel special-casing) */
	if (ssl->ssl_backend && !strcmp("schannel", ssl->ssl_backend) &&
	    !ssl->schannel_use_ssl_cainfo) {
		curl_easy_setopt(curl, CURLOPT_CAINFO, NULL);
		curl_easy_setopt(curl, CURLOPT_PROXY_CAINFO, NULL);
	} else {
		if (ssl->ssl_ca_info)
			curl_easy_setopt(curl, CURLOPT_CAINFO, ssl->ssl_ca_info);
		if (po->proxy_ssl_ca_info)
			curl_easy_setopt(curl, CURLOPT_PROXY_CAINFO,
					 po->proxy_ssl_ca_info);
	}

	/* Low-speed abort */
	if (opts->low_speed_limit > 0 && opts->low_speed_time > 0) {
		curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT,
				 opts->low_speed_limit);
		curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME,
				 opts->low_speed_time);
	}

	/* Redirects */
	curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 20L);
	curl_easy_setopt(curl, CURLOPT_POSTREDIR, (long)CURL_REDIR_POST_ALL);

	/* Protocol restrictions */
#ifdef GIT_CURL_HAVE_CURLOPT_PROTOCOLS_STR
	{
		struct strbuf buf = STRBUF_INIT;
		get_allowed_protocols(0, &buf);
		curl_easy_setopt(curl, CURLOPT_REDIR_PROTOCOLS_STR, buf.buf);
		strbuf_reset(&buf);
		get_allowed_protocols(-1, &buf);
		curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, buf.buf);
		strbuf_release(&buf);
	}
#else
	curl_easy_setopt(curl, CURLOPT_REDIR_PROTOCOLS,
			 get_allowed_protocols(0, NULL));
	curl_easy_setopt(curl, CURLOPT_PROTOCOLS,
			 get_allowed_protocols(-1, NULL));
#endif

	/* User agent */
	curl_easy_setopt(curl, CURLOPT_USERAGENT,
			 opts->user_agent ? opts->user_agent : git_user_agent());

	/* FTP */
	if (opts->ftp_no_epsv)
		curl_easy_setopt(curl, CURLOPT_FTP_USE_EPSV, 0L);
	if (ssl->ssl_try)
		curl_easy_setopt(curl, CURLOPT_USE_SSL, CURLUSESSL_TRY);

	/* Proxy */
	if (po->proxy_url && po->proxy_url[0] == '\0') {
		curl_easy_setopt(curl, CURLOPT_PROXY, "");
	} else if (po->proxy_url) {
		struct strbuf proxy_host = STRBUF_INIT;

		if (starts_with(po->proxy_url, "socks5h"))
			curl_easy_setopt(curl, CURLOPT_PROXYTYPE,
					 (long)CURLPROXY_SOCKS5_HOSTNAME);
		else if (starts_with(po->proxy_url, "socks5"))
			curl_easy_setopt(curl, CURLOPT_PROXYTYPE,
					 (long)CURLPROXY_SOCKS5);
		else if (starts_with(po->proxy_url, "socks4a"))
			curl_easy_setopt(curl, CURLOPT_PROXYTYPE,
					 (long)CURLPROXY_SOCKS4A);
		else if (starts_with(po->proxy_url, "socks"))
			curl_easy_setopt(curl, CURLOPT_PROXYTYPE,
					 (long)CURLPROXY_SOCKS4);
		else if (starts_with(po->proxy_url, "https")) {
			curl_easy_setopt(curl, CURLOPT_PROXYTYPE,
					 (long)CURLPROXY_HTTPS);
			if (po->proxy_ssl_cert)
				curl_easy_setopt(curl, CURLOPT_PROXY_SSLCERT,
						 po->proxy_ssl_cert);
			if (po->proxy_ssl_key)
				curl_easy_setopt(curl, CURLOPT_PROXY_SSLKEY,
						 po->proxy_ssl_key);
			if (has_proxy_cert_password(client))
				curl_easy_setopt(curl, CURLOPT_PROXY_KEYPASSWD,
						 client->proxy_cert_auth.password);
		}

		if (strstr(po->proxy_url, "://"))
			credential_from_url(&client->proxy_auth, po->proxy_url);
		else {
			struct strbuf url = STRBUF_INIT;
			strbuf_addf(&url, "http://%s", po->proxy_url);
			credential_from_url(&client->proxy_auth, url.buf);
			strbuf_release(&url);
		}

		if (!client->proxy_auth.host)
			die("Invalid proxy URL '%s'", po->proxy_url);

		strbuf_addstr(&proxy_host, client->proxy_auth.host);
		if (client->proxy_auth.path) {
			curl_version_info_data *ver =
				curl_version_info(CURLVERSION_NOW);

			if (ver->version_num < 0x075400)
				die("libcurl 7.84 or later is required to "
				    "support paths in proxy URLs");
			if (!starts_with(client->proxy_auth.protocol, "socks"))
				die("Invalid proxy URL '%s': only SOCKS proxies "
				    "support paths", po->proxy_url);
			if (strcasecmp(client->proxy_auth.host, "localhost"))
				die("Invalid proxy URL '%s': host must be "
				    "localhost if a path is present",
				    po->proxy_url);

			strbuf_addch(&proxy_host, '/');
			strbuf_add_percentencode(&proxy_host,
						 client->proxy_auth.path, 0);
		}
		curl_easy_setopt(curl, CURLOPT_PROXY, proxy_host.buf);
		strbuf_release(&proxy_host);

		{
			char *no_proxy = NULL;
			set_from_env(&no_proxy, "NO_PROXY");
			set_from_env(&no_proxy, "no_proxy");
			curl_easy_setopt(curl, CURLOPT_NOPROXY, no_proxy);
			free(no_proxy);
		}
	}
	init_curl_proxy_auth(client, curl);

	/* TCP keepalive */
	curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
	if (opts->tcp_keepidle > -1)
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE,
				 opts->tcp_keepidle);
	if (opts->tcp_keepintvl > -1)
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL,
				 opts->tcp_keepintvl);
#ifdef GIT_CURL_HAVE_CURLOPT_TCP_KEEPCNT
	if (opts->tcp_keepcnt > -1)
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPCNT, opts->tcp_keepcnt);
#endif

	/* Tracing */
	{
		static struct trace_context tctx = { .trace_data = 1,
						     .trace_redact = 1 };
		if (getenv("GIT_CURL_VERBOSE")) {
			trace_override_envvar(&trace_curl, "1");
			tctx.trace_data = 0;
		}
		if (getenv("GIT_TRACE_CURL_NO_DATA"))
			tctx.trace_data = 0;
		if (!git_env_bool("GIT_TRACE_REDACT", 1))
			tctx.trace_redact = 0;
		setup_curl_tracing(curl, &tctx);
	}

	return curl;
}

/* ================================================================== */
/*  Curl callback helpers                                              */
/* ================================================================== */

static size_t write_to_strbuf(char *ptr, size_t eltsize, size_t nmemb,
			      void *userdata)
{
	size_t size = eltsize * nmemb;
	struct strbuf *buf = userdata;
	strbuf_add(buf, ptr, size);
	return nmemb;
}

static size_t write_null(char *ptr UNUSED, size_t eltsize UNUSED,
			 size_t nmemb, void *data UNUSED)
{
	return nmemb;
}

/*
 * Collects WWW-Authenticate headers from the response, handling
 * line folding per RFC 7230.  Stored in `client->auth.wwwauth_headers`.
 */
struct wwwauth_context {
	struct credential *cred;
};

static inline int is_hdr_continuation(const char *ptr, size_t size)
{
	return size && (*ptr == ' ' || *ptr == '\t');
}

static size_t header_wwwauth_cb(char *ptr, size_t eltsize, size_t nmemb,
				void *userdata)
{
	size_t size = eltsize * nmemb;
	struct wwwauth_context *ctx = userdata;
	struct strvec *values = &ctx->cred->wwwauth_headers;
	struct strbuf buf = STRBUF_INIT;
	const char *val;
	size_t val_len;

	if (skip_iprefix_mem(ptr, size, "www-authenticate:", &val, &val_len)) {
		strbuf_add(&buf, val, val_len);
		strbuf_trim(&buf);
		strvec_push(values, buf.buf);
		ctx->cred->header_is_last_match = 1;
		goto exit;
	}

	if (ctx->cred->header_is_last_match && is_hdr_continuation(ptr, size)) {
		strbuf_add(&buf, ptr, size);
		strbuf_trim(&buf);

		if (!values->nr) {
			BUG("should have at least one existing header value");
		} else if (buf.len) {
			char *prev = xstrdup(values->v[values->nr - 1]);
			const char *sp = *prev ? " " : "";
			strvec_pop(values);
			strvec_pushf(values, "%s%s%s", prev, sp, buf.buf);
			free(prev);
		}
		goto exit;
	}

	ctx->cred->header_is_last_match = 0;

	if (skip_iprefix_mem(ptr, size, "http/", &val, &val_len))
		strvec_clear(values);

exit:
	strbuf_release(&buf);
	return size;
}

/* ================================================================== */
/*  String-list to curl_slist conversion                               */
/* ================================================================== */

static struct curl_slist *string_list_to_curl_slist(
		const struct string_list *sl)
{
	struct curl_slist *result = NULL;
	const struct string_list_item *item;

	for_each_string_list_item(item, sl)
		result = curl_slist_append(result, item->string);
	return result;
}

/* ================================================================== */
/*  Default headers (from extra_headers config)                        */
/* ================================================================== */

static struct curl_slist *build_default_headers(
		const struct http_client_options *opts)
{
	struct curl_slist *headers = NULL;
	const struct string_list_item *item;

	for_each_string_list_item(item, &opts->extra_headers)
		headers = curl_slist_append(headers, item->string);

	return headers;
}

/* ================================================================== */
/*  Client init / cleanup                                              */
/* ================================================================== */

struct http_client *http_client_init(struct repository *repo,
				     const struct http_client_options *opts)
{
	struct http_client *client;
	struct http_client_options effective;

	/* Copy opts so we can fill defaults */
	if (opts)
		memcpy(&effective, opts, sizeof(effective));
	else
		memset(&effective, 0, sizeof(effective));

	if (effective.max_requests < 1)
		effective.max_requests = DEFAULT_HTTP_CLIENT_MAX_REQUESTS;

	git_curl_global_init(effective.ssl.ssl_backend);

	CALLOC_ARRAY(client, 1);
	client->repo = repo;
	memcpy(&client->opts, &effective, sizeof(effective));
	client->auth = (struct credential)CREDENTIAL_INIT;
	client->proxy_auth = (struct credential)CREDENTIAL_INIT;
	client->cert_auth = (struct credential)CREDENTIAL_INIT;
	client->proxy_cert_auth = (struct credential)CREDENTIAL_INIT;
	client->auth_methods = CURLAUTH_ANY;

	client->multi = curl_multi_init();
	if (!client->multi)
		die("curl_multi_init failed");

	client->default_headers = build_default_headers(&client->opts);
	client->host_resolutions = string_list_to_curl_slist(
					&client->opts.host_resolutions);
	client->curl_template = build_curl_template(client);

	return client;
}

static void clear_credential(struct credential *cred)
{
	if (cred->password) {
		memset(cred->password, 0, strlen(cred->password));
		FREE_AND_NULL(cred->password);
	}
	credential_clear(cred);
}

void http_client_cleanup(struct http_client *client)
{
	if (!client)
		return;

	if (client->curl_template)
		curl_easy_cleanup(client->curl_template);
	if (client->multi)
		curl_multi_cleanup(client->multi);

	git_curl_global_cleanup();

	curl_slist_free_all(client->default_headers);
	curl_slist_free_all(client->host_resolutions);
	free(client->accept_language);

	clear_credential(&client->auth);
	clear_credential(&client->proxy_auth);
	clear_credential(&client->cert_auth);
	clear_credential(&client->proxy_cert_auth);

	http_client_options_release(&client->opts);
	free(client);
}

/* ================================================================== */
/*  Executing a single request                                         */
/* ================================================================== */

static CURLcode curlinfo_strbuf(CURL *curl, CURLINFO info, struct strbuf *buf)
{
	char *ptr;
	CURLcode ret;

	strbuf_reset(buf);
	ret = curl_easy_getinfo(curl, info, &ptr);
	if (!ret && ptr)
		strbuf_addstr(buf, ptr);
	return ret;
}

static int missing_target(long http_code, CURLcode curl_result)
{
	return (curl_result == CURLE_FILE_COULDNT_READ_FILE) ||
	       (http_code == 404 && curl_result == CURLE_HTTP_RETURNED_ERROR) ||
	       (http_code == 550 && curl_result == CURLE_FTP_COULDNT_RETR_FILE);
}

static void normalize_result(CURLcode *result, long http_code,
			     char *errorstr, size_t errorlen)
{
	if (*result == CURLE_OK && http_code >= 300) {
		*result = CURLE_HTTP_RETURNED_ERROR;
		xsnprintf(errorstr, errorlen,
			  "The requested URL returned error: %ld", http_code);
	}
}

static int classify_result(struct http_client *client,
			   struct http_response *resp,
			   long auth_avail, long http_connectcode)
{
	CURLcode curl_code = (CURLcode)resp->curl_result;

	normalize_result(&curl_code, resp->http_code,
			 resp->error_message, sizeof(resp->error_message));
	resp->curl_result = (int)curl_code;

	if (curl_code == CURLE_OK) {
		credential_approve(client->repo, &client->auth);
		credential_approve(client->repo, &client->proxy_auth);
		credential_approve(client->repo, &client->cert_auth);
		return HTTP_CLIENT_OK;
	}

	if (curl_code == CURLE_SSL_CERTPROBLEM) {
		credential_reject(client->repo, &client->cert_auth);
		return HTTP_CLIENT_NOAUTH;
	}

	if (curl_code == CURLE_SSL_PINNEDPUBKEYNOTMATCH)
		return HTTP_CLIENT_NOMATCHPUBLICKEY;

	if (missing_target(resp->http_code, curl_code))
		return HTTP_CLIENT_MISSING_TARGET;

	if (resp->http_code == 401) {
		if ((client->auth.username && client->auth.password) ||
		    (client->auth.authtype && client->auth.credential)) {
			if (client->auth.multistage) {
				credential_clear_secrets(&client->auth);
				return HTTP_CLIENT_REAUTH;
			}
			credential_reject(client->repo, &client->auth);
			if (always_auth_proactively(client))
				client->opts.proactive_auth = HTTP_CLIENT_AUTH_NONE;
			return HTTP_CLIENT_NOAUTH;
		} else {
			client->auth_methods &= ~CURLAUTH_GSSNEGOTIATE;
			if (auth_avail) {
				client->auth_methods &= auth_avail;
				client->auth_methods_restricted = 1;
			}
			return HTTP_CLIENT_REAUTH;
		}
	}

	if (http_connectcode == 407)
		credential_reject(client->repo, &client->proxy_auth);

	if (!resp->error_message[0])
		strlcpy(resp->error_message,
			curl_easy_strerror(curl_code),
			sizeof(resp->error_message));

	return HTTP_CLIENT_ERROR;
}

/*
 * Execute one HTTP request (no auth retry).
 */
static int execute_request(struct http_client *client,
			   const struct http_request *req,
			   struct http_response *resp)
{
	CURL *curl;
	CURLMcode mc;
	struct curl_slist *headers;
	struct strbuf pragma = STRBUF_INIT;
	const char *accept_language;
	struct wwwauth_context wwwauth = { .cred = &client->auth };
	fd_set readfds, writefds, excfds;
	int max_fd, finished = 0, num_transfers;
	struct timeval select_timeout;

	/* Duplicate the template handle for this request */
	curl = curl_easy_duphandle(client->curl_template);
	if (!curl)
		die("curl_easy_duphandle failed");
	client->session_count++;

	/* Reset per-request options */
	curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, NULL);
	curl_easy_setopt(curl, CURLOPT_READFUNCTION, NULL);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, NULL);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, -1L);
	curl_easy_setopt(curl, CURLOPT_UPLOAD, 0L);
	curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
	curl_easy_setopt(curl, CURLOPT_RANGE, NULL);
	curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, resp->error_message);

	/* Cookie handling */
	if (client->opts.cookie_file &&
	    !strcmp(client->opts.cookie_file, "-")) {
		warning(_("refusing to read cookies from http.cookiefile '-'"));
		FREE_AND_NULL(client->opts.cookie_file);
	}
	curl_easy_setopt(curl, CURLOPT_COOKIEFILE, client->opts.cookie_file);
	if (client->opts.save_cookies) {
		if (!client->opts.cookie_file || !client->opts.cookie_file[0]) {
			warning(_("ignoring http.savecookies for empty "
				  "http.cookiefile"));
		} else {
			curl_easy_setopt(curl, CURLOPT_COOKIEJAR,
					 client->opts.cookie_file);
		}
	}

	/* Redirect policy */
	if (client->opts.follow == HTTP_CLIENT_FOLLOW_ALWAYS)
		curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
	else
		curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L);

	/* IP resolution */
	curl_easy_setopt(curl, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_WHATEVER);
	curl_easy_setopt(curl, CURLOPT_RESOLVE, client->host_resolutions);

	/* Auth */
	curl_easy_setopt(curl, CURLOPT_HTTPAUTH, client->auth_methods);
	if (client->auth.password || client->auth.credential ||
	    empty_auth_usable(client))
		init_curl_http_auth(client, curl);

	/* HTTP method */
	switch (req->method) {
	case HTTP_CLIENT_HEAD:
		curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
		break;
	case HTTP_CLIENT_POST:
		curl_easy_setopt(curl, CURLOPT_POST, 1L);
		if (req->body) {
			curl_easy_setopt(curl, CURLOPT_POSTFIELDS,
					 req->body->buf);
			curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE,
					 (curl_off_t)req->body->len);
		}
		break;
	case HTTP_CLIENT_PUT:
		curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
		break;
	case HTTP_CLIENT_CUSTOM:
		curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST,
				 req->custom_method);
		break;
	case HTTP_CLIENT_GET:
	default:
		curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
		break;
	}

	/* URL */
	curl_easy_setopt(curl, CURLOPT_URL, req->url);

	/* Build headers list (clone cached defaults) */
	headers = NULL;
	{
		struct curl_slist *h;
		for (h = client->default_headers; h; h = h->next)
			headers = curl_slist_append(headers, h->data);
	}

	strbuf_addstr(&pragma, "Pragma:");
	if (req->no_cache)
		strbuf_addstr(&pragma, " no-cache");
	headers = curl_slist_append(headers, pragma.buf);

	accept_language = http_client_accept_language(client);
	if (accept_language)
		headers = curl_slist_append(headers, accept_language);

	if (req->extra_headers) {
		const struct string_list_item *item;
		for_each_string_list_item(item, req->extra_headers)
			headers = curl_slist_append(headers, item->string);
	}

	headers = append_auth_header(&client->auth, headers);

	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	/* Initial redirect follow for initial_request */
	if (req->initial_request &&
	    client->opts.follow == HTTP_CLIENT_FOLLOW_INITIAL)
		curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

	/* Write callback */
	if (req->method == HTTP_CLIENT_HEAD) {
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_null);
	} else {
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_to_strbuf);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp->body);
	}

	/* Header callback for WWW-Authenticate */
	curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, header_wwwauth_cb);
	curl_easy_setopt(curl, CURLOPT_HEADERDATA, &wwwauth);

	/* Accept encoding (gzip, etc.) */
	curl_easy_setopt(curl, CURLOPT_ENCODING, "");

	/* Don't fail on HTTP errors — we classify them ourselves */
	curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0L);

	/* Add to multi handle and run */
	mc = curl_multi_add_handle(client->multi, curl);
	if (mc != CURLM_OK && mc != CURLM_CALL_MULTI_PERFORM) {
		warning("curl_multi_add_handle failed: %s",
			curl_multi_strerror(mc));
		curl_slist_free_all(headers);
		strbuf_release(&pragma);
		curl_easy_cleanup(curl);
		resp->status = HTTP_CLIENT_START_FAILED;
		return HTTP_CLIENT_START_FAILED;
	}

	/* Drive the transfer to completion */
	curl_multi_perform(client->multi, &num_transfers);

	while (!finished) {
		CURLMsg *msg;
		int msgs_in_queue;
		long curl_timeout;

		/* Check for completed transfers */
		while ((msg = curl_multi_info_read(client->multi,
						   &msgs_in_queue))) {
			if (msg->msg == CURLMSG_DONE && msg->easy_handle == curl) {
				resp->curl_result = msg->data.result;
				finished = 1;
			}
		}
		if (finished)
			break;

		/* Wait with select() */
		curl_multi_timeout(client->multi, &curl_timeout);
		if (curl_timeout == 0) {
			curl_multi_perform(client->multi, &num_transfers);
			continue;
		} else if (curl_timeout == -1) {
			select_timeout.tv_sec  = 0;
			select_timeout.tv_usec = 50000;
		} else {
			select_timeout.tv_sec  = curl_timeout / 1000;
			select_timeout.tv_usec = (curl_timeout % 1000) * 1000;
		}

		max_fd = -1;
		FD_ZERO(&readfds);
		FD_ZERO(&writefds);
		FD_ZERO(&excfds);
		curl_multi_fdset(client->multi, &readfds, &writefds,
				 &excfds, &max_fd);

		if (max_fd < 0 &&
		    (select_timeout.tv_sec > 0 ||
		     select_timeout.tv_usec > 50000)) {
			select_timeout.tv_sec  = 0;
			select_timeout.tv_usec = 50000;
		}

		select(max_fd + 1, &readfds, &writefds, &excfds,
		       &select_timeout);
		curl_multi_perform(client->multi, &num_transfers);
	}

	/* Gather results */
	{
		long auth_avail = 0;
		long http_connectcode = 0;

		curl_easy_getinfo(curl, CURLINFO_HTTP_CODE, &resp->http_code);
		curl_easy_getinfo(curl, CURLINFO_HTTPAUTH_AVAIL, &auth_avail);
		curl_easy_getinfo(curl, CURLINFO_HTTP_CONNECTCODE,
				  &http_connectcode);

		/* Content-type */
		{
			struct strbuf raw = STRBUF_INIT;
			curlinfo_strbuf(curl, CURLINFO_CONTENT_TYPE, &raw);
			if (raw.len)
				extract_content_type(&raw, &resp->content_type,
						     &resp->charset);
			strbuf_release(&raw);
		}

		/* Effective URL */
		curlinfo_strbuf(curl, CURLINFO_EFFECTIVE_URL,
				&resp->effective_url);

		/* Classify the result */
		resp->status = classify_result(client, resp,
					       auth_avail, http_connectcode);
	}

	/* Cleanup */
	curl_multi_remove_handle(client->multi, curl);
	curl_easy_cleanup(curl);
	curl_slist_free_all(headers);
	strbuf_release(&pragma);
	client->session_count--;

	return resp->status;
}

/*
 * Execute with authentication retry (up to 3 attempts on 401).
 */
int http_client_send(struct http_client *client,
		     const struct http_request *req,
		     struct http_response *resp)
{
	int attempts = 3;
	int ret;

	if (always_auth_proactively(client))
		credential_fill(client->repo, &client->auth, 1);

	ret = execute_request(client, req, resp);

	while (ret == HTTP_CLIENT_REAUTH && --attempts > 0) {
		strbuf_reset(&resp->body);

		credential_fill(client->repo, &client->auth, 1);

		/* Reset response for retry */
		resp->http_code = 0;
		resp->curl_result = 0;
		resp->error_message[0] = '\0';
		strbuf_reset(&resp->content_type);
		strbuf_reset(&resp->charset);
		strbuf_reset(&resp->effective_url);

		ret = execute_request(client, req, resp);
	}

	return ret;
}

/* ================================================================== */
/*  Convenience wrappers                                               */
/* ================================================================== */

int http_client_get(struct http_client *client,
		    const char *url,
		    const struct http_request *template,
		    struct http_response *resp)
{
	struct http_request req;

	if (template)
		memcpy(&req, template, sizeof(req));
	else
		memset(&req, 0, sizeof(req));

	req.method = HTTP_CLIENT_GET;
	req.url = url;

	return http_client_send(client, &req, resp);
}

int http_client_head(struct http_client *client,
		     const char *url,
		     const struct http_request *template,
		     struct http_response *resp)
{
	struct http_request req;

	if (template)
		memcpy(&req, template, sizeof(req));
	else
		memset(&req, 0, sizeof(req));

	req.method = HTTP_CLIENT_HEAD;
	req.url = url;

	return http_client_send(client, &req, resp);
}

int http_client_post(struct http_client *client,
		     const char *url,
		     const struct strbuf *body,
		     const struct http_request *template,
		     struct http_response *resp)
{
	struct http_request req;

	if (template)
		memcpy(&req, template, sizeof(req));
	else
		memset(&req, 0, sizeof(req));

	req.method = HTTP_CLIENT_POST;
	req.url = url;
	req.body = body;

	return http_client_send(client, &req, resp);
}

/* ================================================================== */
/*  Response cleanup                                                   */
/* ================================================================== */

void http_response_release(struct http_response *resp)
{
	if (!resp)
		return;
	strbuf_release(&resp->body);
	strbuf_release(&resp->content_type);
	strbuf_release(&resp->charset);
	strbuf_release(&resp->effective_url);
}
