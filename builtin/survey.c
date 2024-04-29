#include "builtin.h"
#include "config.h"
#include "json-writer.h"
#include "object-store.h"
#include "parse-options.h"
#include "progress.h"
#include "ref-filter.h"
#include "refs.h"
#include "strvec.h"
#include "trace2.h"

static const char * const survey_usage[] = {
	N_("git survey [<options>])"),
	NULL,
};

static struct progress *survey_progress = NULL;
static uint64_t survey_progress_total = 0;

struct survey_refs_wanted {
	int want_all_refs; /* special override */

	int want_branches;
	int want_tags;
	int want_remotes;
	int want_detached;
	int want_other; /* see FILTER_REFS_OTHERS -- refs/notes/, refs/stash/ */
	int want_prefetch;
	/*
	 * TODO consider adding flags for:
	 *   refs/pull/
	 *   refs/changes/
	 */
};

static struct strvec survey_vec_refs_wanted = STRVEC_INIT;

/*
 * The set of refs that we will search if the user doesn't select
 * any on the command line.
 */
static struct survey_refs_wanted refs_if_unspecified = {
	.want_all_refs = 0,

	.want_branches = 1,
	.want_tags = 1,
	.want_remotes = 1,
	.want_detached = 0,
	.want_other = 0,
	.want_prefetch = 0,
};

struct survey_opts {
	int verbose;
	int show_progress;
	struct survey_refs_wanted refs;
};

static struct survey_opts survey_opts = {
	.verbose = 0,
	.show_progress = -1, /* defaults to isatty(2) */

	.refs.want_all_refs = -1,

	.refs.want_branches = -1, /* default these to undefined */
	.refs.want_tags = -1,
	.refs.want_remotes = -1,
	.refs.want_detached = -1,
	.refs.want_other = -1,
	.refs.want_prefetch = -1,
};

/*
 * After parsing the command line arguments, figure out which refs we
 * should scan.
 *
 * If ANY were given in positive sense, then we ONLY include them and
 * do not use the builtin values.
 */
static void fixup_refs_wanted(void)
{
	struct survey_refs_wanted *rw = &survey_opts.refs;

	/*
	 * `--all-refs` overrides and enables everything.
	 */
	if (rw->want_all_refs == 1) {
		rw->want_branches = 1;
		rw->want_tags = 1;
		rw->want_remotes = 1;
		rw->want_detached = 1;
		rw->want_other = 1;
		rw->want_prefetch = 1;
		return;
	}

	/*
	 * If none of the `--<ref-type>` were given, we assume all
	 * of the builtin unspecified values.
	 */
	if (rw->want_branches == -1 &&
	    rw->want_tags == -1 &&
	    rw->want_remotes == -1 &&
	    rw->want_detached == -1 &&
	    rw->want_other == -1 &&
	    rw->want_prefetch == -1) {
		*rw = refs_if_unspecified;
		return;
	}

	/*
	 * Since we only allow positive boolean values on the command
	 * line, we will only have true values where they specified
	 * a `--<ref-type>`.
	 *
	 * So anything that still has an unspecified value should be
	 * set to false.
	 */
	if (rw->want_branches == -1)
		rw->want_branches = 0;
	if (rw->want_tags == -1)
		rw->want_tags = 0;
	if (rw->want_remotes == -1)
		rw->want_remotes = 0;
	if (rw->want_detached == -1)
		rw->want_detached = 0;
	if (rw->want_other == -1)
		rw->want_other = 0;
	if (rw->want_prefetch == -1)
		rw->want_prefetch = 0;
}

static struct option survey_options[] = {
	OPT__VERBOSE(&survey_opts.verbose, N_("verbose output")),
	OPT_BOOL(0, "progress", &survey_opts.show_progress, N_("show progress")),

	OPT_BOOL_F(0, "all-refs", &survey_opts.refs.want_all_refs, N_("include all refs"),        PARSE_OPT_NONEG),

	OPT_BOOL_F(0, "branches", &survey_opts.refs.want_branches, N_("include branches"),        PARSE_OPT_NONEG),
	OPT_BOOL_F(0, "tags",     &survey_opts.refs.want_tags,     N_("include tags"),            PARSE_OPT_NONEG),
	OPT_BOOL_F(0, "remotes",  &survey_opts.refs.want_remotes,  N_("include remotes"),         PARSE_OPT_NONEG),
	OPT_BOOL_F(0, "detached", &survey_opts.refs.want_detached, N_("include detached HEAD"),   PARSE_OPT_NONEG),
	OPT_BOOL_F(0, "other",    &survey_opts.refs.want_other,    N_("include notes and stash"), PARSE_OPT_NONEG),
	OPT_BOOL_F(0, "prefetch", &survey_opts.refs.want_prefetch, N_("include prefetch"),        PARSE_OPT_NONEG),

	OPT_END(),
};

static int survey_load_config_cb(const char *var, const char *value,
				 const struct config_context *ctx, void *pvoid)
{
	if (!strcmp(var, "survey.verbose")) {
		survey_opts.verbose = git_config_bool(var, value);
		return 0;
	}
	if (!strcmp(var, "survey.progress")) {
		survey_opts.show_progress = git_config_bool(var, value);
		return 0;
	}

	/*
	 * TODO Check for other survey-specific key/value pairs.
	 */

	return git_default_config(var, value, ctx, pvoid);
}

static void survey_load_config(void)
{
	git_config(survey_load_config_cb, NULL);
}

/*
 * Stats on the set of refs that we found.
 */
struct survey_stats_refs {
	uint32_t cnt_total;
	uint32_t cnt_lightweight_tags;
	uint32_t cnt_annotated_tags;
	uint32_t cnt_branches;
	uint32_t cnt_remotes;
	uint32_t cnt_detached;
	uint32_t cnt_other;
	uint32_t cnt_prefetch;

	uint32_t cnt_symref;

	uint32_t cnt_packed;
	uint32_t cnt_loose;

	/*
	 * Measure the length of the refnames.  We can look for potential
	 * platform limits.  The sum may help us estimate the size of a
	 * haves/wants conversation, since each refname and a SHA must be
	 * transmitted.
	 */
	size_t len_max_refname;
	size_t len_sum_refnames;
};

struct survey_stats {
	struct survey_stats_refs refs;
};

static struct survey_stats survey_stats = { 0 };

static void do_load_refs(struct ref_array *ref_array)
{
	struct ref_filter filter = REF_FILTER_INIT;
	struct ref_sorting *sorting;
	struct string_list sorting_options = STRING_LIST_INIT_DUP;

	string_list_append(&sorting_options, "objectname");
	sorting = ref_sorting_options(&sorting_options);

	if (survey_opts.refs.want_branches)
		strvec_push(&survey_vec_refs_wanted, "refs/heads/");
	if (survey_opts.refs.want_tags)
		strvec_push(&survey_vec_refs_wanted, "refs/tags/");
	if (survey_opts.refs.want_remotes)
		strvec_push(&survey_vec_refs_wanted, "refs/remotes/");
	if (survey_opts.refs.want_detached)
		strvec_push(&survey_vec_refs_wanted, "HEAD");
	if (survey_opts.refs.want_other) {
		strvec_push(&survey_vec_refs_wanted, "refs/notes/");
		strvec_push(&survey_vec_refs_wanted, "refs/stash/");
	}
	if (survey_opts.refs.want_prefetch)
		strvec_push(&survey_vec_refs_wanted, "refs/prefetch/");

	filter.name_patterns = survey_vec_refs_wanted.v;
	filter.ignore_case = 0;
#if 1
	filter.match_as_path = 1;
#else
	filter.match_as = REF_MATCH_PATH_PREFIX;
#endif

	if (survey_opts.show_progress) {
		survey_progress_total = 0;
		survey_progress = start_sparse_progress(_("Scanning refs..."), 0);
	}

	filter_refs(ref_array, &filter, FILTER_REFS_KIND_MASK);

	if (survey_opts.show_progress) {
		survey_progress_total = ref_array->nr;
		display_progress(survey_progress, survey_progress_total);
	}

	ref_array_sort(sorting, ref_array);

	if (survey_opts.show_progress)
		stop_progress(&survey_progress);

	ref_filter_clear(&filter);
	ref_sorting_release(sorting);
}

/*
 * Calculate stats on the set of refs that we found.
 */
static void do_calc_stats_refs(struct ref_array *ref_array)
{
	struct survey_stats_refs *prs = &survey_stats.refs;
	int k;

	for (k = 0; k < ref_array->nr; k++) {
		struct ref_array_item *p = ref_array->items[k];
		struct object_id peeled;
		size_t len;

		prs->cnt_total++;

		/*
		 * Classify the ref using the `kind` value.  Note that
		 * p->kind was populated by `ref_kind_from_refname()`
		 * based strictly on the refname.  This only knows about
		 * the basic stock categories and returns FILTER_REFS_OTHERS
		 * for notes, stashes, and any custom namespaces (like
		 * "refs/pulls/" or "refs/prefetch/").
		 */
		switch (p->kind) {
		case FILTER_REFS_TAGS:
			if (!peel_iterated_oid(&p->objectname, &peeled))
				prs->cnt_annotated_tags++;
			else
				prs->cnt_lightweight_tags++;
			break;
		case FILTER_REFS_BRANCHES:
			prs->cnt_branches++;
			break;
		case FILTER_REFS_REMOTES:
			prs->cnt_remotes++;
			break;
		case FILTER_REFS_OTHERS:
			if (starts_with(p->refname, "refs/prefetch/"))
				prs->cnt_prefetch++;
			else
				prs->cnt_other++;
			break;
		case FILTER_REFS_DETACHED_HEAD:
			prs->cnt_detached++;
			break;
		default:
			break;
		}

		/*
		 * SymRefs are somewhat orthogonal to the above
		 * classification (e.g. "HEAD" --> detached
		 * and "refs/remotes/origin/HEAD" --> remote) so
		 * our totals will already include them.
		 */
		if (p->flag & REF_ISSYMREF)
			prs->cnt_symref++;

		/*
		 * Where/how is the ref stored in GITDIR.
		 */
		if (p->flag & REF_ISPACKED)
			prs->cnt_packed++;
		else
			prs->cnt_loose++;

		len = strlen(p->refname);
		prs->len_sum_refnames += len;

		if (len > prs->len_max_refname)
			prs->len_max_refname = len;
	}
}

/*
 * The REFS phase:
 *
 * Load the set of requested refs and assess them for scalablity problems.
 * Use that set to start a treewalk to all reachable objects and assess
 * them.
 *
 * This data will give us insights into the repository itself (the number
 * of refs, the size and shape of the DAG, the number and size of the
 * objects).
 *
 * Theoretically, this data is independent of the on-disk representation
 * (e.g. independent of packing concerns).
 */
static void survey_phase_refs(void)
{
	struct ref_array ref_array = { 0 };

	trace2_region_enter("survey", "phase/refs", the_repository);
	do_load_refs(&ref_array);
	trace2_region_leave("survey", "phase/refs", the_repository);

	do_calc_stats_refs(&ref_array);

	ref_array_clear(&ref_array);
}

static void survey_json(struct json_writer *jw, int pretty)
{
	struct survey_stats_refs *prs = &survey_stats.refs;

	jw_object_begin(jw, pretty);
	{
		jw_object_inline_begin_object(jw, "refs");
		{
			jw_object_intmax(jw, "count", prs->cnt_total);

			jw_object_inline_begin_object(jw, "count_by_type");
			{
				if (survey_opts.refs.want_branches)
					jw_object_intmax(jw, "branches", prs->cnt_branches);
				if (survey_opts.refs.want_tags) {
					jw_object_intmax(jw, "lightweight_tags", prs->cnt_lightweight_tags);
					jw_object_intmax(jw, "annotated_tags", prs->cnt_annotated_tags);
				}
				if (survey_opts.refs.want_remotes)
					jw_object_intmax(jw, "remotes", prs->cnt_remotes);
				if (survey_opts.refs.want_detached)
					jw_object_intmax(jw, "detached", prs->cnt_detached);
				if (survey_opts.refs.want_other)
					jw_object_intmax(jw, "other", prs->cnt_other);

				/*
				 * Technically, refs/prefetch/ (and any
				 * other custom namespace) refs are just
				 * hidden branches, but we don't include
				 * them in the above basic categories.
				 */
				if (survey_opts.refs.want_prefetch)
					jw_object_intmax(jw, "prefetch", prs->cnt_prefetch);

				/*
				 * SymRefs are somewhat orthogonal to
				 * the above classification
				 * (e.g. "HEAD" --> detached and
				 * "refs/remotes/origin/HEAD" -->
				 * remote) so the above classified
				 * counts will already include them,
				 * but it is less confusing to display
				 * them here than to create a whole
				 * new section.
				 */
				if (prs->cnt_symref)
					jw_object_intmax(jw, "symrefs", prs->cnt_symref);
			}
			jw_end(jw);

			jw_object_inline_begin_object(jw, "count_by_storage");
			{
				jw_object_intmax(jw, "loose_refs", prs->cnt_loose);
				jw_object_intmax(jw, "packed_refs", prs->cnt_packed);
			}
			jw_end(jw);

			jw_object_inline_begin_object(jw, "refname_length");
			{
				jw_object_intmax(jw, "max", prs->len_max_refname);
				jw_object_intmax(jw, "sum", prs->len_sum_refnames);
			}
			jw_end(jw);

			jw_object_inline_begin_array(jw, "requested");
			{
				for (k = 0; k < survey_vec_refs_wanted.nr; k++)
					jw_array_string(jw, survey_vec_refs_wanted.v[k]);
			}
			jw_end(jw);
		}
		jw_end(jw);
	}
	jw_end(jw);
}

static void survey_print_results(void)
{
	struct json_writer jw = JSON_WRITER_INIT;

	survey_json(&jw, 1);
	printf("%s\n", jw.json.buf);
	jw_release(&jw);
}

int cmd_survey(int argc, const char **argv, const char *prefix)
{
	prepare_repo_settings(the_repository);
	survey_load_config();

	argc = parse_options(argc, argv, prefix, survey_options, survey_usage, 0);

	if (survey_opts.show_progress < 0)
		survey_opts.show_progress = isatty(2);
	fixup_refs_wanted();

	survey_phase_refs();

	if (trace2_is_enabled()) {
		struct json_writer jw = JSON_WRITER_INIT;

		survey_json(&jw, 0);
		trace2_data_json("survey", the_repository, "results", &jw);
		jw_release(&jw);
	}

	survey_print_results();

	strvec_clear(&survey_vec_refs_wanted);

	return 0;
}
