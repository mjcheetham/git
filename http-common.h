#ifndef HTTP_COMMON_H
#define HTTP_COMMON_H

/*
 * Shared curl global initialization for http.c and http-client.c.
 *
 * Both modules need curl_global_init() / curl_global_cleanup(), and a
 * process may use both simultaneously.  This module ref-counts the
 * calls so that global state is initialised exactly once and torn down
 * only when the last user is done.
 *
 * Callers must ensure that init and cleanup calls are made from a
 * single thread (curl_global_init is not thread-safe).
 */

/*
 * Increment the global refcount, calling curl_global_init() on the
 * first call.  `ssl_backend` may be NULL; when non-NULL the SSL
 * backend is selected before the first init (it is an error to pass
 * different non-NULL values across calls).
 *
 * Dies on failure.
 */
void git_curl_global_init(const char *ssl_backend);

/*
 * Decrement the global refcount, calling curl_global_cleanup() when
 * it reaches zero.
 */
void git_curl_global_cleanup(void);

#endif /* HTTP_COMMON_H */
