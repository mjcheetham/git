#include "git-compat-util.h"
#include "git-curl-compat.h"
#include "http2.h"
#include "version.h"
#include "strbuf.h"
#include "string-list.h"
#include "strvec.h"

static struct slot {
	unsigned in_use:1;
	unsigned finished:1;
	CURL *curl;
	CURLcode curl_result;
	struct http_response *response;
	struct slot *next;
} *queue_head;

static CURLM *curlm;
static CURL *curl_default;

/* number of CURL handles created */
static int curl_session_count;

/* minimum number of curl sessions to maintain */
static int min_curl_sessions = 1;

/* number of active slots */
static int active_requests;

static struct slot *get_slot(void)
{
	struct slot *slot = queue_head;
	struct slot *new_slot;

	/* Find the first available existing slot */
	while (slot != NULL && slot->in_use)
		slot = slot->next;

	/* If no slot is available, create a new one */
	if (!slot) {
		new_slot = xmalloc(sizeof(*slot));
		new_slot->curl = NULL;
		new_slot->in_use = 0;
		new_slot->next = NULL;

		slot = queue_head;
		if (!slot) {
			queue_head = new_slot;
		} else {
			while (slot->next != NULL)
				slot = slot->next;
			slot->next = new_slot;
		}
		slot = new_slot;
	}

	/* If the slot has no curl handle, create one from the default handle */
	if (!slot->curl) {
		slot->curl = curl_easy_duphandle(curl_default);
		curl_session_count++;
	}

	/* TODO: consider moving this to start_slot() */
	active_requests++;

	slot->in_use = 1;
	slot->next = NULL;
	slot->curl_result = CURLE_OK;

	return slot;
}

/* Start processing a slot using the curl multi interface. */
static int start_slot(struct slot *slot)
{
	CURLMcode curlm_result = curl_multi_add_handle(curlm, slot->curl);
	int num_transfers;

	// TODO: consider moving active_requests-- from get_slot() to here

	if (curlm_result != CURLM_OK &&
	    curlm_result != CURLM_CALL_MULTI_PERFORM) {
		warning("curl_multi_add_handle failed: %s",
			curl_multi_strerror(curlm_result));
		active_requests--;
		slot->in_use = 0;
		return 0;
	}

	/* We know there is something to do since we just added something. */
	curl_multi_perform(curlm, &num_transfers);
	return 1;
}

static void update_response(CURL *curl, CURLcode curl_result, struct http_response *res)
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

static void process_curl_messages(void)
{
	int num_messages;
	struct slot *slot;
	CURLMsg *curl_message = curl_multi_info_read(curlm, &num_messages);

	while (curl_message != NULL) {
		if (curl_message->msg == CURLMSG_DONE) {
			CURLcode curl_result = curl_message->data.result;

			/* Find the slot that this message pertains to */
			slot = queue_head;
			while (slot != NULL &&
			       slot->curl != curl_message->easy_handle)
				slot = slot->next;

			if (slot) {
				/* Remove slot's handle from the multi handle */
				curl_multi_remove_handle(curlm, slot->curl);

				/* Store the curl result code on the slot */
				slot->curl_result = curl_result;

				/* Store results now as we will no longer be
				 * able to access them from the curl handle
				 * after we mark it as not in use.
				 */
				update_response(slot->curl, curl_result, slot->response);

				/* Mark the slot as finished */
				active_requests--;
				slot->in_use = 0;
				slot->finished = 1;
			} else {
				fprintf(stderr, "Recieved DONE message for unknown request!\n");
			}

		} else {
			fprintf(stderr, "Unknown CURL message received: %d\n",
				(int)curl_message->msg);
		}

		curl_message = curl_multi_info_read(curlm, &num_messages);
	}
}

static void cleanup_slots(void)
{
	struct slot *slot = queue_head;
	while (slot != NULL) {
		if (!slot->in_use && slot->curl != NULL &&
		    curl_session_count > min_curl_sessions) {
			curl_easy_cleanup(slot->curl);
			slot->curl = NULL;
			curl_session_count--;
		}
		slot = slot->next;
	}
}

static void step_slots(void)
{
	int num_transfers;
	CURLMcode curlm_result;

	do {
		curlm_result = curl_multi_perform(curlm, &num_transfers);
	} while (curlm_result == CURLM_CALL_MULTI_PERFORM);

	/* If there are fewer running transfers than we added to the curl multi
	 * handle then one of them must have finished.
	 */
	if (num_transfers < active_requests) {
		process_curl_messages();
		cleanup_slots();
		// TODO: do we need the fill functions?
		//fill_active_slots();
	}
}

static void run_slot(struct slot *slot)
{
	fd_set readfds;
	fd_set writefds;
	fd_set excfds;
	int max_fd;
	struct timeval select_timeout;

	while (!slot->finished) {
		step_slots();

		if (slot->in_use) {
			long curl_timeout;
			curl_multi_timeout(curlm, &curl_timeout);
			if (curl_timeout == 0) {
				continue;
			} else if (curl_timeout == -1) {
				/* no timeout configured; default to 50ms */
				select_timeout.tv_sec = 0;
				select_timeout.tv_usec = 50000;
			} else {
				select_timeout.tv_sec = curl_timeout / 1000;
				select_timeout.tv_usec = (curl_timeout % 1000) * 1000;
			}

			max_fd = -1;
			FD_ZERO(&readfds);
			FD_ZERO(&writefds);
			FD_ZERO(&excfds);
			curl_multi_fdset(curlm, &readfds, &writefds, &excfds, &max_fd);

			/*
			 * It can happen that curl_multi_timeout returns a pathologically
			 * long timeout when curl_multi_fdset returns no file descriptors
			 * to read.  See commit message for more details.
			 */
			if (max_fd < 0 &&
			    (select_timeout.tv_sec > 0 ||
			     select_timeout.tv_usec > 50000)) {
				select_timeout.tv_sec  = 0;
				select_timeout.tv_usec = 50000;
			}

			select(max_fd+1, &readfds, &writefds, &excfds, &select_timeout);
		}
	}
}

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
	curlm = curl_multi_init();
}

void http_cleanup(void)
{
	curl_multi_cleanup(curlm);
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

int http_request(struct http_request *req, struct http_response *res)
{
	struct curl_slist *headers = NULL;
	struct slot *slot = get_slot();

	slot->response = res;

	/* Set request URL and method. */
	curl_easy_setopt(slot->curl, CURLOPT_URL, req->url);
	switch (req->method)
	{
	case HTTP_GET:
		curl_easy_setopt(slot->curl, CURLOPT_HTTPGET, 1L);
		break;
	case HTTP_HEAD:
		curl_easy_setopt(slot->curl, CURLOPT_NOBODY, 1L);
		break;
	case HTTP_POST:
		curl_easy_setopt(slot->curl, CURLOPT_POST, 1L);
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
	curl_easy_setopt(slot->curl, CURLOPT_HTTPHEADER, headers);

#ifndef GIT_CURL_HAVE_HEADER_API
	/* Handle response headers via callback function. */
	if (res->headers) {
		curl_easy_setopt(slot->curl, CURLOPT_HEADERFUNCTION, fwrite_headers);
		curl_easy_setopt(slot->curl, CURLOPT_HEADERDATA, res->headers);
	}
#endif

	/* Request content. */
	switch (req->data_type)
	{
	case HTTP_DATA_NONE:
		curl_easy_setopt(slot->curl, CURLOPT_READFUNCTION, fread_null);
		break;
	case HTTP_DATA_CALLBACK:
		curl_easy_setopt(slot->curl, CURLOPT_READFUNCTION, req->data.callback.fn);
		curl_easy_setopt(slot->curl, CURLOPT_READDATA, req->data.callback.userdata);
		break;
	case HTTP_DATA_FILE:
		curl_easy_setopt(slot->curl, CURLOPT_READFUNCTION, fread);
		curl_easy_setopt(slot->curl, CURLOPT_READFUNCTION, res->data.file);
		break;
	case HTTP_DATA_STRBUF:
		curl_easy_setopt(slot->curl, CURLOPT_READFUNCTION, fread_strbuf);
		curl_easy_setopt(slot->curl, CURLOPT_READDATA, req->data.strbuf.buf);
		if (req->data.strbuf.content_type) {
			char *h = xstrfmt("Content-Type: %s", req->data.strbuf.content_type);
			headers = curl_slist_append(headers, h);
			free(h);
		}
		break;
	case HTTP_DATA_POSTFIELDS:
		curl_easy_setopt(slot->curl, CURLOPT_POSTFIELDS, req->data.postfields.data);
		/* TODO: expose and call xcurl_off_t */
		curl_easy_setopt(slot->curl, CURLOPT_POSTFIELDSIZE_LARGE, req->data.postfields.len);
		break;
	default:
		BUG("unsupported HTTP data type");
		break;
	}

	/* Response content. */
	switch (res->data_type)
	{
	case HTTP_DATA_NONE:
		curl_easy_setopt(slot->curl, CURLOPT_WRITEFUNCTION, fwrite_null);
		break;
	case HTTP_DATA_CALLBACK:
		curl_easy_setopt(slot->curl, CURLOPT_WRITEFUNCTION, res->data.callback.fn);
		curl_easy_setopt(slot->curl, CURLOPT_WRITEDATA, res->data.callback.userdata);
		break;
	case HTTP_DATA_FILE:
		curl_easy_setopt(slot->curl, CURLOPT_WRITEFUNCTION, fwrite);
		curl_easy_setopt(slot->curl, CURLOPT_WRITEDATA, res->data.file);
		break;
	case HTTP_DATA_STRBUF:
		curl_easy_setopt(slot->curl, CURLOPT_WRITEFUNCTION, fwrite_strbuf);
		curl_easy_setopt(slot->curl, CURLOPT_WRITEDATA, res->data.strbuf);
		break;
	default:
		BUG("unsupported HTTP data type");
		break;
	}

	/* Make the request! */
	if (!start_slot(slot)) {
		// TOOD: handle global error state
		// xsnprintf(curl_errorstr, sizeof(curl_errorstr),
		// 	  "failed to start HTTP request");
		return HTTP_START_FAILED;
	}

	run_slot(slot);

	return slot->curl_result;
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
