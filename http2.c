#include "git-curl-compat.h"
#include "http2.h"
#include "config.h"

/* cURL proxy */
static const char *curl_http_proxy;
static const char *http_proxy_authmethod;
static const char *http_proxy_ssl_cert;
static const char *http_proxy_ssl_key;
static const char *http_proxy_ssl_ca_info;

static int curl_ssl_verify = -1;
static const char *ssl_key;
static const char *ssl_capath;
static const char *curl_no_proxy;
#ifdef GIT_CURL_HAVE_CURLOPT_PINNEDPUBLICKEY
static const char *ssl_pinnedkey;
#endif
static const char *ssl_cainfo;

static long curl_low_speed_limit = -1;
static long curl_low_speed_time = -1;

static CURLM *curlm;
static CURL *curl_default;

static int test(void)
{
	struct http_response *response;
	struct http_request request = HTTP_REQUEST(HTTP_GET, "http://example.com");

	curl_slist_append(request.headers, "X-Foo: Bar");

	response = http2_send(&request);

	if (response->status_code == HTTP_OK)
		return 0;

	return -1;
}

struct http_response *http_send(struct http_request *request)
{
}

static int http_options(const char *var, const char *value, void *cb)
{
	if (!strcmp("http.version", var)) {
		return git_config_string(&curl_http_version, var, value);
	}
	if (!strcmp("http.sslverify", var)) {
		curl_ssl_verify = git_config_bool(var, value);
		return 0;
	}
	if (!strcmp("http.sslcipherlist", var))
		return git_config_string(&ssl_cipherlist, var, value);
	if (!strcmp("http.sslversion", var))
		return git_config_string(&ssl_version, var, value);
	if (!strcmp("http.sslcert", var))
		return git_config_pathname(&ssl_cert, var, value);
	if (!strcmp("http.sslkey", var))
		return git_config_pathname(&ssl_key, var, value);
	if (!strcmp("http.sslcapath", var))
		return git_config_pathname(&ssl_capath, var, value);
	if (!strcmp("http.sslcainfo", var))
		return git_config_pathname(&ssl_cainfo, var, value);
	if (!strcmp("http.sslcertpasswordprotected", var)) {
		ssl_cert_password_required = git_config_bool(var, value);
		return 0;
	}
	if (!strcmp("http.ssltry", var)) {
		curl_ssl_try = git_config_bool(var, value);
		return 0;
	}
	if (!strcmp("http.sslbackend", var)) {
		free(http_ssl_backend);
		http_ssl_backend = xstrdup_or_null(value);
		return 0;
	}

	if (!strcmp("http.schannelcheckrevoke", var)) {
		http_schannel_check_revoke = git_config_bool(var, value);
		return 0;
	}

	if (!strcmp("http.schannelusesslcainfo", var)) {
		http_schannel_use_ssl_cainfo = git_config_bool(var, value);
		return 0;
	}

	if (!strcmp("http.minsessions", var)) {
		min_curl_sessions = git_config_int(var, value);
		if (min_curl_sessions > 1)
			min_curl_sessions = 1;
		return 0;
	}
	if (!strcmp("http.maxrequests", var)) {
		max_requests = git_config_int(var, value);
		return 0;
	}
	if (!strcmp("http.lowspeedlimit", var)) {
		curl_low_speed_limit = (long)git_config_int(var, value);
		return 0;
	}
	if (!strcmp("http.lowspeedtime", var)) {
		curl_low_speed_time = (long)git_config_int(var, value);
		return 0;
	}

	if (!strcmp("http.noepsv", var)) {
		curl_ftp_no_epsv = git_config_bool(var, value);
		return 0;
	}
	if (!strcmp("http.proxy", var))
		return git_config_string(&curl_http_proxy, var, value);

	if (!strcmp("http.proxyauthmethod", var))
		return git_config_string(&http_proxy_authmethod, var, value);

	if (!strcmp("http.proxysslcert", var))
		return git_config_string(&http_proxy_ssl_cert, var, value);

	if (!strcmp("http.proxysslkey", var))
		return git_config_string(&http_proxy_ssl_key, var, value);

	if (!strcmp("http.proxysslcainfo", var))
		return git_config_string(&http_proxy_ssl_ca_info, var, value);

	if (!strcmp("http.proxysslcertpasswordprotected", var)) {
		proxy_ssl_cert_password_required = git_config_bool(var, value);
		return 0;
	}

	if (!strcmp("http.cookiefile", var))
		return git_config_pathname(&curl_cookie_file, var, value);
	if (!strcmp("http.savecookies", var)) {
		curl_save_cookies = git_config_bool(var, value);
		return 0;
	}

	if (!strcmp("http.postbuffer", var)) {
		http_post_buffer = git_config_ssize_t(var, value);
		if (http_post_buffer < 0)
			warning(_("negative value for http.postBuffer; defaulting to %d"), LARGE_PACKET_MAX);
		if (http_post_buffer < LARGE_PACKET_MAX)
			http_post_buffer = LARGE_PACKET_MAX;
		return 0;
	}

	if (!strcmp("http.useragent", var))
		return git_config_string(&user_agent, var, value);

	if (!strcmp("http.emptyauth", var)) {
		if (value && !strcmp("auto", value))
			curl_empty_auth = -1;
		else
			curl_empty_auth = git_config_bool(var, value);
		return 0;
	}

	if (!strcmp("http.delegation", var)) {
#ifdef CURLGSSAPI_DELEGATION_FLAG
		return git_config_string(&curl_deleg, var, value);
#else
		warning(_("Delegation control is not supported with cURL < 7.22.0"));
		return 0;
#endif
	}

	if (!strcmp("http.pinnedpubkey", var)) {
#ifdef GIT_CURL_HAVE_CURLOPT_PINNEDPUBLICKEY
		return git_config_pathname(&ssl_pinnedkey, var, value);
#else
		warning(_("Public key pinning not supported with cURL < 7.39.0"));
		return 0;
#endif
	}

	if (!strcmp("http.extraheader", var)) {
		if (!value) {
			return config_error_nonbool(var);
		} else if (!*value) {
			string_list_clear(&extra_http_headers, 0);
		} else {
			string_list_append(&extra_http_headers, value);
		}
		return 0;
	}

	if (!strcmp("http.curloptresolve", var)) {
		if (!value) {
			return config_error_nonbool(var);
		} else if (!*value) {
			curl_slist_free_all(host_resolutions);
			host_resolutions = NULL;
		} else {
			host_resolutions = curl_slist_append(host_resolutions, value);
		}
		return 0;
	}

	if (!strcmp("http.followredirects", var)) {
		if (value && !strcmp(value, "initial"))
			http_follow_config = HTTP_FOLLOW_INITIAL;
		else if (git_config_bool(var, value))
			http_follow_config = HTTP_FOLLOW_ALWAYS;
		else
			http_follow_config = HTTP_FOLLOW_NONE;
		return 0;
	}

	/* Fall back on the default ones */
	return git_default_config(var, value, cb);
}

static CURL *get_curl_handle(void)
{
	CURL *result = curl_easy_init();

	if (!result)
		die("curl_easy_init failed");

	return result;
}

void http2_init(struct remote *remote, const char* url)
{
	char *low_speed_limit;
	char *low_speed_time;
	char *normalized_url;
	struct urlmatch_config config = URLMATCH_CONFIG_INIT;

	config.section = "http";
	config.key = NULL;
	config.collect_fn = http_options;
	config.cascade_fn = git_default_config;
	config.cb = NULL;

	http_is_verbose = 0;
	normalized_url = url_normalize(url, &config.url);

	git_config(urlmatch_config_entry, &config);
	free(normalized_url);
	string_list_clear(&config.vars, 1);

#ifdef GIT_CURL_HAVE_CURLSSLSET_NO_BACKENDS
	if (http_ssl_backend) {
		const curl_ssl_backend **backends;
		struct strbuf buf = STRBUF_INIT;
		int i;

		switch (curl_global_sslset(-1, http_ssl_backend, &backends)) {
		case CURLSSLSET_UNKNOWN_BACKEND:
			strbuf_addf(&buf, _("Unsupported SSL backend '%s'. "
					    "Supported SSL backends:"),
					    http_ssl_backend);
			for (i = 0; backends[i]; i++)
				strbuf_addf(&buf, "\n\t%s", backends[i]->name);
			die("%s", buf.buf);
		case CURLSSLSET_NO_BACKENDS:
			die(_("Could not set SSL backend to '%s': "
			      "cURL was built without SSL backends"),
			    http_ssl_backend);
		case CURLSSLSET_TOO_LATE:
			die(_("Could not set SSL backend to '%s': already set"),
			    http_ssl_backend);
		case CURLSSLSET_OK:
			break; /* Okay! */
		}
	}
#endif

	if (curl_global_init(CURL_GLOBAL_ALL) != CURLE_OK)
		die("curl_global_init failed");

	if (remote && remote->http_proxy)
		curl_http_proxy = xstrdup(remote->http_proxy);

	if (remote)
		var_override(&http_proxy_authmethod, remote->http_proxy_authmethod);

	{
		char *http_max_requests = getenv("GIT_HTTP_MAX_REQUESTS");
		if (http_max_requests)
			max_requests = atoi(http_max_requests);
	}

	curlm = curl_multi_init();
	if (!curlm)
		die("curl_multi_init failed");

	if (getenv("GIT_SSL_NO_VERIFY"))
		curl_ssl_verify = 0;

	set_from_env(&ssl_cert, "GIT_SSL_CERT");
	set_from_env(&ssl_key, "GIT_SSL_KEY");
	set_from_env(&ssl_capath, "GIT_SSL_CAPATH");
	set_from_env(&ssl_cainfo, "GIT_SSL_CAINFO");

	set_from_env(&user_agent, "GIT_HTTP_USER_AGENT");

	low_speed_limit = getenv("GIT_HTTP_LOW_SPEED_LIMIT");
	if (low_speed_limit)
		curl_low_speed_limit = strtol(low_speed_limit, NULL, 10);
	low_speed_time = getenv("GIT_HTTP_LOW_SPEED_TIME");
	if (low_speed_time)
		curl_low_speed_time = strtol(low_speed_time, NULL, 10);

	if (curl_ssl_verify == -1)
		curl_ssl_verify = 1;

	curl_session_count = 0;
	if (max_requests < 1)
		max_requests = DEFAULT_MAX_REQUESTS;

	set_from_env(&http_proxy_ssl_cert, "GIT_PROXY_SSL_CERT");
	set_from_env(&http_proxy_ssl_key, "GIT_PROXY_SSL_KEY");
	set_from_env(&http_proxy_ssl_ca_info, "GIT_PROXY_SSL_CAINFO");

	if (getenv("GIT_PROXY_SSL_CERT_PASSWORD_PROTECTED"))
		proxy_ssl_cert_password_required = 1;

	if (getenv("GIT_CURL_FTP_NO_EPSV"))
		curl_ftp_no_epsv = 1;


	curl_default = get_curl_handle();
}

void http2_cleanup(void)
{
	curl_easy_cleanup(curl_default);
}
