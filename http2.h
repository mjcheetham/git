#ifndef HTTP2_H
#define HTTP2_H

#include <curl/curl.h>
#include <curl/easy.h>

#include "strbuf.h"
#include "remote.h"

enum http_method {
	HTTP_GET = 0,
	HTTP_HEAD,
	HTTP_POST,
	HTTP_PUT,
	HTTP_DELETE,
};

struct http_request {
	enum http_method method;
	const char *url;
	struct curl_slist *headers;
};

#define HTTP_REQUEST(method, url) { method, url, NULL }
#define HTTP_OK 200

struct http_response {
	long status_code;
};

struct http_response *http_send(struct http_request *request);

void http2_init(struct remote *remote, const char* url);
void http2_cleanup(void);

#endif /* HTTP_H */
