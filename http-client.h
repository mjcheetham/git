#ifndef HTTP_CLIENT_H
#define HTTP_CLIENT_H

#include "strbuf.h"
#include "string-list.h"

struct repository;

/*
 * A modern, context-based HTTP client for Git.
 *
 * Unlike http.c, all state is held in an `http_client` context—no global
 * variables, and no curl types leak into the public interface.  The
 * typical flow is:
 *
 *   1. Populate an `http_client_options` (or zero-init for defaults).
 *   2. Call http_client_init() to obtain an `http_client`.
 *   3. Issue requests with http_client_send() or the convenience
 *      wrappers http_client_get() / http_client_head() / http_client_post().
 *   4. Inspect the returned `http_response`, then release it.
 *   5. When done, call http_client_cleanup().
 *
 *
 * == Quick-start example: simple GET =====================================
 *
 *   struct http_client_options opts = HTTP_CLIENT_OPTIONS_INIT;
 *   struct http_client *client;
 *   struct http_response resp = HTTP_RESPONSE_INIT;
 *   int ret;
 *
 *   // Load settings from git config & environment, then create client.
 *   http_client_options_load(&opts, repo, "https://example.com");
 *   client = http_client_init(repo, &opts);
 *   http_client_options_release(&opts);   // client owns its own copy
 *
 *   // Perform a GET.
 *   ret = http_client_get(client, "https://example.com/info/refs", NULL, &resp);
 *   if (ret == HTTP_CLIENT_OK)
 *       printf("body: %s\n", resp.body.buf);
 *   else
 *       error("HTTP request failed: %s", resp.error_message);
 *
 *   http_response_release(&resp);
 *   http_client_cleanup(client);
 *
 *
 * == Custom request with headers =========================================
 *
 *   struct http_request req = HTTP_REQUEST_INIT;
 *   struct http_response resp = HTTP_RESPONSE_INIT;
 *   struct string_list hdrs = STRING_LIST_INIT_NODUP;
 *
 *   string_list_append(&hdrs,
 *       "Content-Type: application/x-git-upload-pack-request");
 *
 *   req.url = "https://example.com/git-upload-pack";
 *   req.method = HTTP_CLIENT_POST;
 *   req.body = &my_strbuf;          // request body (a struct strbuf *)
 *   req.no_cache = 1;
 *   req.extra_headers = &hdrs;
 *
 *   ret = http_client_send(client, &req, &resp);
 *
 *   string_list_clear(&hdrs, 0);
 *   http_response_release(&resp);
 *
 *
 * == HEAD request (check existence) ======================================
 *
 *   struct http_response resp = HTTP_RESPONSE_INIT;
 *
 *   if (http_client_head(client, url, NULL, &resp) == HTTP_CLIENT_OK)
 *       printf("exists, content-type: %s\n", resp.content_type.buf);
 *
 *   http_response_release(&resp);
 *
 *
 * == Inspecting response details =========================================
 *
 *   The http_response struct provides:
 *     - resp.status          HTTP_CLIENT_OK / _ERROR / _MISSING_TARGET / …
 *     - resp.http_code       Raw HTTP status (200, 404, …)
 *     - resp.body            Response body as a strbuf
 *     - resp.content_type    Normalised content-type ("text/plain")
 *     - resp.charset         Charset parameter ("utf-8") or empty
 *     - resp.effective_url   Final URL after any redirects
 *     - resp.error_message   Human-readable error on failure
 *
 *   Authentication retries (HTTP 401) are handled automatically—up to
 *   3 attempts—using the credential helpers configured for the repo.
 *
 *
 * == Migrating from the old http.h API ===================================
 *
 *   Old (http.h, global state):
 *
 *     http_init(remote, url, proactive_auth);
 *     ret = http_get_strbuf(url, &buf, &get_options);
 *     http_cleanup();
 *
 *   New (http-client.h, context-based):
 *
 *     struct http_client_options opts = HTTP_CLIENT_OPTIONS_INIT;
 *     http_client_options_load(&opts, repo, url);
 *     opts.proactive_auth = HTTP_CLIENT_AUTH_IF_CREDENTIALS;
 *
 *     client = http_client_init(repo, &opts);
 *     http_client_options_release(&opts);
 *
 *     struct http_request req = HTTP_REQUEST_INIT;
 *     req.url = url;
 *     req.no_cache = get_options.no_cache;
 *     req.initial_request = get_options.initial_request;
 *     req.extra_headers = get_options.extra_headers;
 *
 *     struct http_response resp = HTTP_RESPONSE_INIT;
 *     ret = http_client_send(client, &req, &resp);
 *     // resp.body ≈ old strbuf result
 *     // resp.content_type ≈ old get_options.content_type
 *     // resp.effective_url ≈ old get_options.effective_url
 *
 *     http_response_release(&resp);
 *     http_client_cleanup(client);
 */

/* ------------------------------------------------------------------ */
/*  Return codes (compatible with http.h for easier future migration) */
/* ------------------------------------------------------------------ */

#define HTTP_CLIENT_OK              0
#define HTTP_CLIENT_MISSING_TARGET  1
#define HTTP_CLIENT_ERROR           2
#define HTTP_CLIENT_START_FAILED    3
#define HTTP_CLIENT_REAUTH          4
#define HTTP_CLIENT_NOAUTH          5
#define HTTP_CLIENT_NOMATCHPUBLICKEY 6

/* ------------------------------------------------------------------ */
/*  SSL / TLS options                                                  */
/* ------------------------------------------------------------------ */

struct http_client_ssl_options {
	int ssl_verify;         /* -1 = default (verify), 0 = off, 1 = on */
	int ssl_try;            /* attempt SSL even for non-SSL URLs */
	char *ssl_cert;
	char *ssl_cert_type;
	char *ssl_key;
	char *ssl_key_type;
	char *ssl_ca_path;
	char *ssl_ca_info;
	char *ssl_cipher_list;
	char *ssl_version;      /* e.g. "tlsv1.2" */
	char *ssl_pinned_key;
	char *ssl_backend;      /* e.g. "schannel", "openssl" */
	int schannel_check_revoke;   /* 1 = check (default) */
	int schannel_use_ssl_cainfo; /* 0 = don't override cert store */
	int cert_password_required;
};

#define HTTP_CLIENT_SSL_OPTIONS_INIT { \
	.ssl_verify = -1, \
	.schannel_check_revoke = 1, \
}

/* ------------------------------------------------------------------ */
/*  Proxy options                                                      */
/* ------------------------------------------------------------------ */

struct http_client_proxy_options {
	char *proxy_url;          /* http.proxy or env */
	char *proxy_auth_method;  /* basic, digest, negotiate, ntlm, anyauth */
	char *no_proxy;           /* NO_PROXY env value */
	char *proxy_ssl_cert;
	char *proxy_ssl_key;
	char *proxy_ssl_ca_info;
	int proxy_ssl_cert_password_required;
};

#define HTTP_CLIENT_PROXY_OPTIONS_INIT { 0 }

/* ------------------------------------------------------------------ */
/*  Authentication options                                             */
/* ------------------------------------------------------------------ */

enum http_client_proactive_auth {
	HTTP_CLIENT_AUTH_NONE = 0,
	HTTP_CLIENT_AUTH_IF_CREDENTIALS,
	HTTP_CLIENT_AUTH_AUTO,
	HTTP_CLIENT_AUTH_BASIC,
};

/* ------------------------------------------------------------------ */
/*  Redirect-follow policy                                             */
/* ------------------------------------------------------------------ */

enum http_client_follow {
	HTTP_CLIENT_FOLLOW_NONE,
	HTTP_CLIENT_FOLLOW_ALWAYS,
	HTTP_CLIENT_FOLLOW_INITIAL,
};

/* ------------------------------------------------------------------ */
/*  Top-level client options                                           */
/* ------------------------------------------------------------------ */

struct http_client_options {
	/* Connection limits */
	int max_requests;       /* 0 = use DEFAULT_HTTP_CLIENT_MAX_REQUESTS */

	/* Protocol */
	char *http_version;     /* "HTTP/1.1" or "HTTP/2" */

	/* Authentication */
	enum http_client_proactive_auth proactive_auth;
	int empty_auth;                   /* -1=auto, 0=off, 1=on */

	/* Redirect */
	enum http_client_follow follow;

	/* Transfer */
	ssize_t post_buffer;    /* min LARGE_PACKET_MAX */
	char *user_agent;

	/* Timeouts / keepalive */
	long low_speed_limit;   /* bytes/sec, -1 = unset */
	long low_speed_time;    /* seconds, -1 = unset */
	long tcp_keepidle;      /* -1 = unset */
	long tcp_keepintvl;     /* -1 = unset */
	long tcp_keepcnt;       /* -1 = unset */

	/* Cookies */
	char *cookie_file;
	int save_cookies;

	/* Headers */
	struct string_list extra_headers;     /* http.extraHeader values */
	struct string_list host_resolutions;  /* http.curloptResolve values */

	/* Sub-structures */
	struct http_client_ssl_options ssl;
	struct http_client_proxy_options proxy;

	/* FTP (legacy) */
	int ftp_no_epsv;

	/* Verbose / tracing */
	int verbose;
};

#define DEFAULT_HTTP_CLIENT_MAX_REQUESTS 5

#define HTTP_CLIENT_OPTIONS_INIT { \
	.empty_auth = -1, \
	.follow = HTTP_CLIENT_FOLLOW_INITIAL, \
	.low_speed_limit = -1, \
	.low_speed_time = -1, \
	.tcp_keepidle = -1, \
	.tcp_keepintvl = -1, \
	.tcp_keepcnt = -1, \
	.extra_headers = STRING_LIST_INIT_DUP, \
	.host_resolutions = STRING_LIST_INIT_DUP, \
	.ssl = HTTP_CLIENT_SSL_OPTIONS_INIT, \
	.proxy = HTTP_CLIENT_PROXY_OPTIONS_INIT, \
}

/* ------------------------------------------------------------------ */
/*  Client context (opaque — definition is in http-client.c)           */
/* ------------------------------------------------------------------ */

struct http_client;

/*
 * Populate `opts` from git configuration, environment variables, and
 * (optionally) a remote's per-URL overrides.  Call this before
 * http_client_init() if you want the standard git config integration.
 *
 * `url` may be NULL if not yet known.
 */
void http_client_options_load(struct http_client_options *opts,
			      struct repository *repo,
			      const char *url);

/*
 * Release resources inside `opts` (strings, header lists).
 * The struct itself is NOT freed (it is typically stack-allocated).
 */
void http_client_options_release(struct http_client_options *opts);

/*
 * Create a new HTTP client.  `opts` is copied into the client; the
 * caller may release it afterwards.  `repo` is stored by reference and
 * must outlive the client (used for credential helpers).  Returns NULL
 * on failure.
 */
struct http_client *http_client_init(struct repository *repo,
				     const struct http_client_options *opts);

/*
 * Tear down the client, releasing all curl handles and memory.
 */
void http_client_cleanup(struct http_client *client);

/* ------------------------------------------------------------------ */
/*  Request                                                            */
/* ------------------------------------------------------------------ */

enum http_client_method {
	HTTP_CLIENT_GET,
	HTTP_CLIENT_HEAD,
	HTTP_CLIENT_POST,
	HTTP_CLIENT_PUT,
	HTTP_CLIENT_CUSTOM,
};

struct http_request {
	enum http_client_method method;
	const char *url;
	const char *custom_method;   /* only when method == HTTP_CLIENT_CUSTOM */

	/*
	 * Request body for POST / PUT.  Exactly one of `body` (in-memory
	 * strbuf) or neither for bodyless requests.
	 */
	const struct strbuf *body;

	/* Options that mirror http_get_options for compatibility */
	unsigned no_cache:1,
		 initial_request:1;

	/* Additional per-request headers (merged with client defaults) */
	struct string_list *extra_headers;
};

#define HTTP_REQUEST_INIT { .method = HTTP_CLIENT_GET }

/* ------------------------------------------------------------------ */
/*  Response                                                           */
/* ------------------------------------------------------------------ */

#define HTTP_CLIENT_ERROR_SIZE 256

struct http_response {
	int status;            /* HTTP_CLIENT_OK, _ERROR, etc. */
	long http_code;        /* raw HTTP status code (200, 404, …) */
	int curl_result;       /* underlying curl result code */

	struct strbuf body;    /* response body (if captured) */

	struct strbuf content_type;   /* normalised, e.g. "text/plain" */
	struct strbuf charset;        /* e.g. "utf-8" */
	struct strbuf effective_url;  /* after redirects */

	char error_message[HTTP_CLIENT_ERROR_SIZE];
};

#define HTTP_RESPONSE_INIT { \
	.body = STRBUF_INIT, \
	.content_type = STRBUF_INIT, \
	.charset = STRBUF_INIT, \
	.effective_url = STRBUF_INIT, \
}

void http_response_release(struct http_response *resp);

/* ------------------------------------------------------------------ */
/*  Executing requests                                                 */
/* ------------------------------------------------------------------ */

/*
 * Execute a single HTTP request synchronously.  `resp` must be
 * initialised (HTTP_RESPONSE_INIT) before calling.
 *
 * Handles authentication retries automatically (up to 3 attempts).
 * Returns one of the HTTP_CLIENT_* status codes, also stored in
 * resp->status.
 */
int http_client_send(struct http_client *client,
		     const struct http_request *req,
		     struct http_response *resp);

/*
 * Convenience: GET `url` into `resp->body`.
 */
int http_client_get(struct http_client *client,
		    const char *url,
		    const struct http_request *req,
		    struct http_response *resp);

/*
 * Convenience: HEAD `url` (no body captured).
 */
int http_client_head(struct http_client *client,
		     const char *url,
		     const struct http_request *req,
		     struct http_response *resp);

/*
 * Convenience: POST `body` to `url`.
 */
int http_client_post(struct http_client *client,
		     const char *url,
		     const struct strbuf *body,
		     const struct http_request *req,
		     struct http_response *resp);

/* ------------------------------------------------------------------ */
/*  Utility                                                            */
/* ------------------------------------------------------------------ */

/*
 * Build the Accept-Language header value from the user's locale.
 * The returned string is cached in `client` and must not be freed.
 * Returns NULL if no language preference is available.
 */
const char *http_client_accept_language(struct http_client *client);

#endif /* HTTP_CLIENT_H */
