#ifndef HTTP2_H
#define HTTP2_H

#include <curl/curl.h>

enum http_data_type {
	HTTP_DATA_NONE,
	HTTP_DATA_CALLBACK,
	HTTP_DATA_FILE,
	HTTP_DATA_STRBUF,
	HTTP_DATA_POSTFIELDS,
};

struct http_request {
	/* Include the "Pragma: no-cache" header in the request. */
	unsigned no_cache:1;

	/* Do not perform any decompression of response contents. */
	unsigned no_encoding:1;

	/* Do not handle authentication challenges. */
	unsigned no_auth:1;

	/* HTTP request URL. */
	const char *url;

	/* HTTP request method. */
	const enum http_method {
		/* Send a GET request. */
		HTTP_GET,

		/* Send a HEAD request.
		 * The response body will be empty and any content response
		 * handlers will not be called if set.
		 */
		HTTP_HEAD,

		/* Send a POST request. */
		HTTP_POST,
	} method;

	/* Extra headers to include in the request. */
	struct string_list *extra_headers;

	/* Request content. */
	const enum http_data_type data_type;
	union {
		FILE *file;
		struct {
			struct strbuf *buf;
			const char *content_type;
		} strbuf;
		struct {
			const char *data;
			size_t len;
		} postfields;
		struct {
			curl_write_callback fn;
			void *userdata;
		} callback;
	} data;
};

#define HTTP_REQUEST_INIT { 0, 0, 0, NULL, HTTP_GET, NULL, HTTP_DATA_NONE, { NULL } }

struct http_response {
	/* CURL result code. */
	CURLcode curl_result;

	/* HTTP response status code. */
	unsigned long http_status;

	/* HTTP response connect code. */
	unsigned long http_connectcode;

	/* Response content type. */
	const char *content_type;

	/* Response headers. */
	struct strvec *headers;

	/* Response target. */
	const enum http_data_type data_type;
	union {
		FILE *file;
		struct strbuf *strbuf;
		struct {
			curl_write_callback fn;
			void *userdata;
		} callback;
	} data;
};

#define HTTP_RESPONSE_INIT { 0, 0, NULL, NULL, HTTP_DATA_NONE, { NULL } }

void http_init(void);
void http_cleanup(void);

/* Perform an HTTP request with the specified parameters, returning the
 * resulting HTTP status code.
 * Results are stored in the response structure if specified. */
int http_request(struct http_request *, struct http_response *);

#endif
