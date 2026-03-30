#define DISABLE_SIGN_COMPARE_WARNINGS

#include "git-compat-util.h"
#include "git-curl-compat.h"

#include <curl/curl.h>

#include "http-common.h"
#include "gettext.h"
#include "strbuf.h"

/* Guards one-time SSL backend selection and trace setup. */
static int curl_session_started;

void git_curl_global_init(const char *ssl_backend)
{
	if (!curl_session_started && ssl_backend) {
		const curl_ssl_backend **backends;
		struct strbuf buf = STRBUF_INIT;
		int i;

		switch (curl_global_sslset(-1, ssl_backend, &backends)) {
		case CURLSSLSET_UNKNOWN_BACKEND:
			strbuf_addf(&buf,
				    _("Unsupported SSL backend '%s'. "
				      "Supported SSL backends:"),
				    ssl_backend);
			for (i = 0; backends[i]; i++)
				strbuf_addf(&buf, "\n\t%s",
					    backends[i]->name);
			die("%s", buf.buf);
		case CURLSSLSET_NO_BACKENDS:
			die(_("Could not set SSL backend to '%s': "
			      "cURL was built without SSL backends"),
			    ssl_backend);
		case CURLSSLSET_TOO_LATE:
			die(_("Could not set SSL backend to '%s': "
			      "already set"),
			    ssl_backend);
		case CURLSSLSET_OK:
			break;
		}
	}

	if (curl_global_init(CURL_GLOBAL_ALL) != CURLE_OK)
		die("curl_global_init failed");

	if (!curl_session_started) {
#ifdef GIT_CURL_HAVE_GLOBAL_TRACE
		const char *comp = getenv("GIT_TRACE_CURL_COMPONENTS");
		if (comp)
			curl_global_trace(comp);
#endif
		curl_session_started = 1;
	}
}

void git_curl_global_cleanup(void)
{
	curl_global_cleanup();
}
