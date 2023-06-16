#include "git-compat-util.h"
#include "git-curl-compat.h"
#include "http2.h"
#include "version.h"
#include "strbuf.h"
#include "string-list.h"
#include "strvec.h"

static CURL *curl_default;

static CURL *get_curl_handle(void)
{
	CURL *result = curl_easy_init();

	if (!result)
		die("curl_easy_init failed");

	curl_easy_setopt(result, CURLOPT_USERAGENT, git_user_agent());

	return result;
}

void http_init(void)
{
	if (curl_global_init(CURL_GLOBAL_ALL) != CURLE_OK)
		die("curl_global_init failed");

	curl_default = get_curl_handle();
}

void http_cleanup(void)
{
	curl_easy_cleanup(curl_default);
	curl_global_cleanup();
}

static size_t fread_null(char *ptr, size_t eltsize, size_t nmemb, void *strbuf)
{
	return nmemb;
}

static size_t fwrite_null(char *ptr, size_t eltsize, size_t nmemb, void *strbuf)
{
	return nmemb;
}

static size_t fwrite_strbuf(char *ptr, size_t eltsize, size_t nmemb, void *userdata)
{
	size_t size = eltsize * nmemb;
	struct strbuf *buf = userdata;
	strbuf_add(buf, ptr, size);
	return nmemb;
}

static size_t fread_strbuf(char *ptr, size_t eltsize, size_t nmemb, void *userdata)
{
	struct strbuf *buf = userdata;
	size_t size = eltsize * nmemb;
	if (size > buf->len)
		size = buf->len;
	memcpy(ptr, buf->buf, size);
	strbuf_remove(buf, 0, size);
	return size;
}

#ifndef GIT_CURL_HAVE_HEADER_API
/*
 * A folded header continuation line starts with any number of spaces or
 * horizontal tab characters (SP or HTAB) as per RFC 7230 section 3.2.
 * It is not a continuation line if the line starts with any other character.
 */
static inline int is_hdr_continuation(const char *ptr, const size_t size)
{
	return size && (*ptr == ' ' || *ptr == '\t');
}

static size_t fwrite_headers(char *ptr, size_t eltsize, size_t nmemb, void *userdata)
{
	size_t size = eltsize * nmemb;
	struct strvec *values = userdata;
	struct strbuf buf = STRBUF_INIT;
	const char *val;
	size_t val_len;

	/*
	 * Header lines may not come NULL-terminated from libcurl so we must
	 * limit all scans to the maximum length of the header line, or leverage
	 * strbufs for all operations.
	 *
	 * In addition, it is possible that header values can be split over
	 * multiple lines as per RFC 7230. 'Line folding' has been deprecated
	 * but older servers may still emit them. A continuation header field
	 * value is identified as starting with a space or horizontal tab.
	 *
	 * The formal definition of a header field as given in RFC 7230 is:
	 *
	 * header-field   = field-name ":" OWS field-value OWS
	 *
	 * field-name     = token
	 * field-value    = *( field-content / obs-fold )
	 * field-content  = field-vchar [ 1*( SP / HTAB ) field-vchar ]
	 * field-vchar    = VCHAR / obs-text
	 *
	 * obs-fold       = CRLF 1*( SP / HTAB )
	 *                ; obsolete line folding
	 *                ; see Section 3.2.4
	 */

	/*
	 * If this is a HTTP status line and not a header field, this signals
	 * a different HTTP response. libcurl writes all the output of all
	 * response headers of all responses, including redirects.
	 * We only care about the last HTTP request response's headers so clear
	 * the existing array.
	 */
	if (skip_iprefix_mem(ptr, size, "http/", &val, &val_len))
		strvec_clear(values);
	/*
	 * This line is a continuation of the previous header field.
	 * We should append this value to the end of the previously consumed value.
	 */
	else if (is_hdr_continuation(ptr, size)) {
		/*
		 * Trim the CRLF and any leading or trailing from this line.
		 */
		strbuf_add(&buf, ptr, size);
		strbuf_trim(&buf);

		/*
		 * At this point we should always have at least one existing
		 * value, even if it is empty. Do not bother appending the new
		 * value if this continuation header is itself empty.
		 */
		if (!values->nr) {
			BUG("should have at least one existing header value");
		} else if (buf.len) {
			char *prev = xstrdup(values->v[values->nr - 1]);

			/* Join two non-empty values with a single space. */
			const char *const sp = *prev ? " " : "";

			strvec_pop(values);
			strvec_pushf(values, "%s%s%s", prev, sp, buf.buf);
			free(prev);
		}
	} else {
		/* Start of a new header! */

		/*
		 * Strip the CRLF that should be present at the end of each
		 * field as well as any trailing or leading whitespace from the
		 * value.
		 */
		strbuf_add(&buf, ptr, size);
		strbuf_trim(&buf);

		strvec_push(values, buf.buf);
	}

	strbuf_release(&buf);
	return size;
}
#endif

static void handle_response(CURL *curl, CURLcode curl_result, struct http_response *res)
{
	res->curl_result = curl_result;

	/* Capture common results. */
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &res->http_status);
	curl_easy_getinfo(curl, CURLINFO_HTTP_CONNECTCODE, &res->http_connectcode);

#ifdef GIT_CURL_HAVE_HEADER_API
	/* Handle response headers using libcurl header API. */
	if (res->headers) {
		struct curl_header *prev = NULL;
		struct curl_header *hdr = NULL;
		unsigned origin = CURLH_HEADER | CURLH_TRAILER;
		while ((hdr = curl_easy_nextheader(curl, origin, -1, prev))){
			strvec_pushf(res->headers, "%s: %s", hdr->name, hdr->value);
			prev = hdr;
		}
	}
#endif
}

int http_request(struct http_request *req, struct http_response *res)
{
	struct curl_slist *headers = NULL;
	CURL *curl;
	
	curl = curl_easy_duphandle(curl_default);
	if (!curl)
		die("curl_easy_duphandle failed");

	/* Set request URL and method. */
	curl_easy_setopt(curl, CURLOPT_URL, req->url);
	switch (req->method)
	{
	case HTTP_GET:
		curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
		break;
	case HTTP_HEAD:
		curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
		break;
	case HTTP_POST:
		curl_easy_setopt(curl, CURLOPT_POST, 1L);
		break;
	default:
		die("unsupported HTTP method");
	}

	/* Set request flags. */
	if (req->no_cache)
		headers = curl_slist_append(headers, "Pragma: no-cache");
	else
		headers = curl_slist_append(headers, "Pragma:");

	/* Add additional headers and set headers on curl handle. */
	if (req->extra_headers) {
		struct string_list_item *hdr;
		for_each_string_list_item(hdr, req->extra_headers)
			headers = curl_slist_append(headers, hdr->string);
	}
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

#ifndef GIT_CURL_HAVE_HEADER_API
	/* Handle response headers via callback function. */
	if (res->headers) {
		curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, fwrite_headers);
		curl_easy_setopt(curl, CURLOPT_HEADERDATA, res->headers);
	}
#endif

	/* Request content. */
	switch (req->data_type)
	{
	case HTTP_DATA_NONE:
		curl_easy_setopt(curl, CURLOPT_READFUNCTION, fread_null);
		break;
	case HTTP_DATA_CALLBACK:
		curl_easy_setopt(curl, CURLOPT_READFUNCTION, req->data.callback.fn);
		curl_easy_setopt(curl, CURLOPT_READDATA, req->data.callback.userdata);
		break;
	case HTTP_DATA_FILE:
		curl_easy_setopt(curl, CURLOPT_READFUNCTION, fread);
		curl_easy_setopt(curl, CURLOPT_READFUNCTION, res->data.file);
		break;
	case HTTP_DATA_STRBUF:
		curl_easy_setopt(curl, CURLOPT_READFUNCTION, fread_strbuf);
		curl_easy_setopt(curl, CURLOPT_READDATA, req->data.strbuf.buf);
		if (req->data.strbuf.content_type) {
			char *h = xstrfmt("Content-Type: %s", req->data.strbuf.content_type);
			headers = curl_slist_append(headers, h);
			free(h);
		}
		break;
	case HTTP_DATA_POSTFIELDS:
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, req->data.postfields.data);
		/* TODO: expose and call xcurl_off_t */
		curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, req->data.postfields.len);
		break;
	default:
		BUG("unsupported HTTP data type");
		break;
	}

	/* Response content. */
	switch (res->data_type)
	{
	case HTTP_DATA_NONE:
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, fwrite_null);
		break;
	case HTTP_DATA_CALLBACK:
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, res->data.callback.fn);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, res->data.callback.userdata);
		break;
	case HTTP_DATA_FILE:
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, fwrite);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, res->data.file);
		break;
	case HTTP_DATA_STRBUF:
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, fwrite_strbuf);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, res->data.strbuf);
		break;
	default:
		BUG("unsupported HTTP data type");
		break;
	}

	/* Make the request! */
	CURLcode curl_result = curl_easy_perform(curl);

	handle_response(curl, curl_result, res);

	curl_easy_cleanup(curl);
	return curl_result;
}

int cmd_main(int argc, const char **argv)
{
	struct string_list request_headers = STRING_LIST_INIT_DUP;
	const char *fields = "q=hello+world&foo=bar";
	struct http_request request = {
		.url = "https://httpbin.org/post",
		.method = HTTP_POST,
		.extra_headers = &request_headers,
		.data_type = HTTP_DATA_POSTFIELDS,
		.data.postfields = {
			.data = fields,
			.len = strlen(fields),
		}
	};
	struct strvec response_headers = STRVEC_INIT;
	struct strbuf res_buf = STRBUF_INIT;
	struct http_response response = {
		.data_type = HTTP_DATA_STRBUF,
		.data.strbuf = &res_buf,
		.headers = &response_headers
	};

	http_init();

	string_list_append(request.extra_headers, "X-Foo: bar");

	int err = http_request(&request, &response);

	printf("err: %d\n", err);
	printf("curl result: %d\n", response.curl_result);
	printf("http status: %ld\n", response.http_status);
	printf("len(response): %zu\n", res_buf.len);
	printf("response: %s\n", res_buf.buf);
	for (size_t i = 0; i < response_headers.nr; i++)
		printf("header: %s\n", response_headers.v[i]);

	strbuf_release(&res_buf);
	strvec_clear(&response_headers);
	http_cleanup();

	return 0;
}
