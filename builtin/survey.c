#include "builtin.h"
#include "config.h"
#include "environment.h"
#include "hex.h"
#include "json-writer.h"
#include "list-objects.h"
#include "object-name.h"
#include "object-store.h"
#include "parse-options.h"
#include "progress.h"
#include "ref-filter.h"
#include "refs.h"
#include "revision.h"
#include "strbuf.h"
#include "strvec.h"
#include "trace2.h"
#include "tree.h"
#include "tree-walk.h"

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

/*
 * HBIN -- hex binning (histogram bucketing).
 *
 * We create histograms for various counts and sums.  Since we have a
 * wide range of values (objects range in size from 1 to 4G bytes), a
 * linear bucketing is not interesting.  Instead, lets use a
 * log16()-based bucketing.  This gives us a better spread on the low
 * and middle range and a coarse bucketing on the high end.
 *
 * The idea here is that it doesn't matter if you have n 1GB blobs or
 * n/2 1GB blobs and n/2 1.5GB blobs -- either way you have a scaling
 * problem that we want to report on.
 */
#define HBIN_LEN (sizeof(unsigned long) * 2)
#define HBIN_MASK (0xF)
#define HBIN_SHIFT (4)

static int hbin(unsigned long value)
{
	int k;

	for (k = 0; k < HBIN_LEN; k++) {
		if ((value & ~(HBIN_MASK)) == 0)
			return k;
		value >>= HBIN_SHIFT;
	}

	return 0; /* should not happen */
}

/*
 * QBIN -- base4 binning (histogram bucketing).
 *
 * This is the same idea as the above, but we want better granularity
 * in the low end and don't expect as many large values.
 */
#define QBIN_LEN (sizeof(unsigned long) * 4)
#define QBIN_MASK (0x3)
#define QBIN_SHIFT (2)

static int qbin(unsigned long value)
{
	int k;

	for (k = 0; k < QBIN_LEN; k++) {
		if ((value & ~(QBIN_MASK)) == 0)
			return k;
		value >>= (QBIN_SHIFT);
	}

	return 0; /* should not happen */
}

/*
 * histogram bin for objects.
 */
struct obj_hist_bin {
	uint64_t sum_size;      /* sum(object_size) for all objects in this bin */
	uint64_t sum_disk_size; /* sum(on_disk_size) for all objects in this bin */
	uint32_t cnt_seen;      /* number seen in this bin */
};

static void incr_obj_hist_bin(struct obj_hist_bin *pbin,
			       unsigned long object_length,
			       off_t disk_sizep)
{
	pbin->sum_size += object_length;
	pbin->sum_disk_size += disk_sizep;
	pbin->cnt_seen++;
}

/*
 * Common fields for any type of object.
 */
struct survey_stats_base_object {
	uint32_t cnt_seen;

	uint32_t cnt_missing; /* we may have a partial clone. */

	/*
	 * Number of objects grouped by where they are stored on disk.
	 * This is a function of how the ODB is packed.
	 */
	uint32_t cnt_cached;   /* see oi.whence */
	uint32_t cnt_loose;    /* see oi.whence */
	uint32_t cnt_packed;   /* see oi.whence */
	uint32_t cnt_dbcached; /* see oi.whence */

	uint64_t sum_size; /* sum(object_size) */
	uint64_t sum_disk_size; /* sum(disk_size) */

	/*
	 * A histogram of the count of objects, the observed size, and
	 * the on-disk size grouped by the observed size.
	 */
	struct obj_hist_bin size_hbin[HBIN_LEN];
};

/*
 * PBIN -- parent vector binning (histogram bucketing).
 *
 * We create a histogram based upon the number of parents
 * in a commit.  This is a simple linear vector.  It starts
 * at zero for "initial" commits.
 *
 * If a commit has more parents, just put it in the last bin.
 */
#define PVEC_LEN (17)

struct survey_stats_commits {
	struct survey_stats_base_object base;

	/*
	 * Count of commits with k parents.
	 */
	uint32_t parent_cnt_pbin[PVEC_LEN];

	/*
	 * Remember the OID of the commit with the most number of parents.
	 */
	uint32_t max_parents;
	struct object_id oid_max_parents;

	/*
	 * The largest commit.  This is probably just the commit with
	 * the longest commit message.
	 */
	unsigned long size_largest;
	struct object_id oid_largest;
};

/*
 * Stats for reachable trees.
 */
struct survey_stats_trees {
	struct survey_stats_base_object base;

	/*
	 * In the following, nr_entries refers to the number of files or
	 * subdirectories in a tree.  We are interested in how wide the
	 * tree is and if the repo has gigantic directories.
	 */
	uint64_t max_entries; /* max(nr_entries) -- the width of the largest tree */
	struct object_id oid_max_entries; /* OID of the tree with the most entries */

	/*
	 * Computing the sum of the number of entries across all trees
	 * is probably not that interesting.
	 */
	uint64_t sum_entries; /* sum(nr_entries) -- sum across all trees */

	/*
	 * A histogram of the count of trees, the observed size, and
	 * the on-disk size grouped by the number of entries in the tree.
	 */
	struct obj_hist_bin entry_qbin[QBIN_LEN];
};

/*
 * Stats for reachable blobs.
 */
struct survey_stats_blobs {
	struct survey_stats_base_object base;

	/*
	 * Remember the OID of the largest blob.
	 */
	unsigned long size_largest;
	struct object_id oid_largest;
};

struct survey_stats {
	struct survey_stats_refs    refs;
	struct survey_stats_commits commits;
	struct survey_stats_trees   trees;
	struct survey_stats_blobs   blobs;
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
 * Populate a "rev_info" with the OIDs of the REFS of interest.
 * The treewalk will start from all of those starting points
 * and walk backwards in the DAG to get the set of all reachable
 * objects from those starting points.
 */
static void load_rev_info(struct rev_info *rev_info,
			  struct ref_array *ref_array)
{
	unsigned int add_flags = 0;
	int k;

	for (k = 0; k < ref_array->nr; k++) {
		struct ref_array_item *p = ref_array->items[k];
		struct object_id peeled;

		switch (p->kind) {
		case FILTER_REFS_TAGS:
			if (!peel_iterated_oid(&p->objectname, &peeled))
				add_pending_oid(rev_info, NULL, &peeled, add_flags);
			else
				add_pending_oid(rev_info, NULL, &p->objectname, add_flags);
			break;
		case FILTER_REFS_BRANCHES:
			add_pending_oid(rev_info, NULL, &p->objectname, add_flags);
			break;
		case FILTER_REFS_REMOTES:
			add_pending_oid(rev_info, NULL, &p->objectname, add_flags);
			break;
		case FILTER_REFS_OTHERS:
			/*
			 * This may be a note, stash, or custom namespace branch.
			 */
			add_pending_oid(rev_info, NULL, &p->objectname, add_flags);
			break;
		case FILTER_REFS_DETACHED_HEAD:
			add_pending_oid(rev_info, NULL, &p->objectname, add_flags);
			break;
		default:
			break;
		}
	}
}

static int fill_in_base_object(struct survey_stats_base_object *base,
			       struct object *object,
			       enum object_type type_expected,
			       unsigned long *p_object_length,
			       off_t *p_disk_sizep)
{
	struct object_info oi = OBJECT_INFO_INIT;
	unsigned oi_flags = OBJECT_INFO_FOR_PREFETCH;
	unsigned long object_length = 0;
	off_t disk_sizep = 0;
	enum object_type type;
	int hb;

	base->cnt_seen++;

	oi.typep = &type;
	oi.sizep = &object_length;
	oi.disk_sizep = &disk_sizep;

	if (oid_object_info_extended(the_repository, &object->oid, &oi, oi_flags) < 0 ||
	    type != type_expected) {
		base->cnt_missing++;
		return 1;
	}

	switch (oi.whence) {
	case OI_CACHED:
		base->cnt_cached++;
		break;
	case OI_LOOSE:
		base->cnt_loose++;
		break;
	case OI_PACKED:
		base->cnt_packed++;
		break;
	case OI_DBCACHED:
		base->cnt_dbcached++;
		break;
	default:
		break;
	}

	base->sum_size += object_length;
	base->sum_disk_size += disk_sizep;

	hb = hbin(object_length);
	incr_obj_hist_bin(&base->size_hbin[hb], object_length, disk_sizep);

	if (p_object_length)
		*p_object_length = object_length;
	if (p_disk_sizep)
		*p_disk_sizep = disk_sizep;

	return 0;
}

static void traverse_commit_cb(struct commit *commit, void *data)
{
	struct survey_stats_commits *psc = &survey_stats.commits;
	unsigned long object_length;
	unsigned k;

	display_progress(survey_progress, ++survey_progress_total);

	fill_in_base_object(&psc->base, &commit->object, OBJ_COMMIT, &object_length, NULL);

	k = commit_list_count(commit->parents);

	if (k > psc->max_parents) {
		psc->max_parents = k;
		oidcpy(&psc->oid_max_parents, &commit->object.oid);
	}

	if (k >= PVEC_LEN)
		k = PVEC_LEN - 1;
	psc->parent_cnt_pbin[k]++;

	/*
	 * Remember the OID of the single largest commit.  This is
	 * probably just the one with the longest commit message.
	 * Note that this is for parity with `git-sizer` since we
	 * already have a histogram based on the commit size elsewhere.
	 */
	if (object_length > psc->size_largest) {
		psc->size_largest = object_length;
		oidcpy(&psc->oid_largest, &commit->object.oid);
	}
}

static void traverse_object_cb_tree(struct object *obj)
{
	struct survey_stats_trees *pst = &survey_stats.trees;
	unsigned long object_length;
	off_t disk_sizep;
	struct tree_desc desc;
	struct name_entry entry;
	struct tree *tree;
	int nr_entries;
	int qb;

	if (fill_in_base_object(&pst->base, obj, OBJ_TREE, &object_length, &disk_sizep))
		return;

	tree = lookup_tree(the_repository, &obj->oid);
	if (!tree)
		return;
	init_tree_desc(&desc, &obj->oid, tree->buffer, tree->size);
	nr_entries = 0;
	while (tree_entry(&desc, &entry))
		nr_entries++;

	pst->sum_entries += nr_entries;

	if (nr_entries > pst->max_entries) {
		pst->max_entries = nr_entries;
		oidcpy(&pst->oid_max_entries, &obj->oid);
	}

	qb = qbin(nr_entries);
	incr_obj_hist_bin(&pst->entry_qbin[qb], object_length, disk_sizep);
}

static void traverse_object_cb_blob(struct object *obj)
{
	struct survey_stats_blobs *psb = &survey_stats.blobs;
	unsigned long object_length;

	fill_in_base_object(&psb->base, obj, OBJ_BLOB, &object_length, NULL);

	/*
	 * Remember the OID of the single largest blob.
	 */
	if (object_length > psb->size_largest) {
		psb->size_largest = object_length;
		oidcpy(&psb->oid_largest, &obj->oid);
	}
}

static void traverse_object_cb(struct object *obj, const char *name, void *data)
{
	display_progress(survey_progress, ++survey_progress_total);

	switch (obj->type) {
	case OBJ_TREE:
		traverse_object_cb_tree(obj);
		return;
	case OBJ_BLOB:
		traverse_object_cb_blob(obj);
		return;
	case OBJ_TAG:    /* ignore     -- counted when loading REFS */
	case OBJ_COMMIT: /* ignore/bug -- seen in the other callback */
	default:         /* ignore/bug -- unknown type */
		return;
	}
}

/*
 * Treewalk all of the commits and objects reachable from the
 * set of refs.
 */
static void do_treewalk_reachable(struct ref_array *ref_array)
{
	struct rev_info rev_info = REV_INFO_INIT;

	repo_init_revisions(the_repository, &rev_info, NULL);
	rev_info.tree_objects = 1;
	rev_info.blob_objects = 1;
	load_rev_info(&rev_info, ref_array);
	if (prepare_revision_walk(&rev_info))
		die(_("revision walk setup failed"));

	if (survey_opts.show_progress) {
		survey_progress_total = 0;
		survey_progress = start_sparse_progress(_("Walking reachable objects..."), 0);
	}

	traverse_commit_list(&rev_info,
			     traverse_commit_cb,
			     traverse_object_cb,
			     NULL);

	if (survey_opts.show_progress)
		stop_progress(&survey_progress);
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

	trace2_region_enter("survey", "phase/treewalk", the_repository);
	do_treewalk_reachable(&ref_array);
	trace2_region_leave("survey", "phase/treewalk", the_repository);

	do_calc_stats_refs(&ref_array);

	ref_array_clear(&ref_array);
}

#define JW_OBJ_INT_NZ(jw, key, value) do { if (value) jw_object_intmax((jw), (key), (value)); } while (0)

static void write_qbin_json(struct json_writer *jw, const char *label,
			    struct obj_hist_bin qbin[QBIN_LEN])
{
	struct strbuf buf = STRBUF_INIT;
	uint32_t lower = 0;
	uint32_t upper = QBIN_MASK;
	int k;

	jw_object_inline_begin_object(jw, label);
	{
		for (k = 0; k < QBIN_LEN; k++) {
			struct obj_hist_bin *p = &qbin[k];
			uint32_t lower_k = lower;
			uint32_t upper_k = upper;

			lower = upper+1;
			upper = (upper << QBIN_SHIFT) + QBIN_MASK;

			if (!p->cnt_seen)
				continue;

			strbuf_reset(&buf);
			strbuf_addf(&buf, "Q%02d", k);
			jw_object_inline_begin_object(jw, buf.buf);
			{
				jw_object_intmax(jw, "count", p->cnt_seen);
				jw_object_intmax(jw, "sum_size", p->sum_size);
				jw_object_intmax(jw, "sum_disk_size", p->sum_disk_size);

				/* maybe only include these in verbose mode */
				jw_object_intmax(jw, "qbin_lower", lower_k);
				jw_object_intmax(jw, "qbin_upper", upper_k);
			}
			jw_end(jw);
		}
	}
	jw_end(jw);

	strbuf_release(&buf);
}

static void write_hbin_json(struct json_writer *jw, const char *label,
			    struct obj_hist_bin hbin[HBIN_LEN])
{
	struct strbuf buf = STRBUF_INIT;
	uint32_t lower = 0;
	uint32_t upper = HBIN_MASK;
	int k;

	jw_object_inline_begin_object(jw, label);
	{
		for (k = 0; k < HBIN_LEN; k++) {
			struct obj_hist_bin *p = &hbin[k];
			uint32_t lower_k = lower;
			uint32_t upper_k = upper;

			lower = upper+1;
			upper = (upper << HBIN_SHIFT) + HBIN_MASK;

			if (!p->cnt_seen)
				continue;

			strbuf_reset(&buf);
			strbuf_addf(&buf, "H%d", k);
			jw_object_inline_begin_object(jw, buf.buf);
			{
				jw_object_intmax(jw, "count", p->cnt_seen);
				jw_object_intmax(jw, "sum_size", p->sum_size);
				jw_object_intmax(jw, "sum_disk_size", p->sum_disk_size);

				/* maybe only include these in verbose mode */
				jw_object_intmax(jw, "hbin_lower", lower_k);
				jw_object_intmax(jw, "hbin_upper", upper_k);
			}
			jw_end(jw);
		}
	}
	jw_end(jw);

	strbuf_release(&buf);
}

static void write_base_object_json(struct json_writer *jw,
				   struct survey_stats_base_object *base)
{
	jw_object_intmax(jw, "count", base->cnt_seen);

	jw_object_intmax(jw, "sum_size", base->sum_size);
	jw_object_intmax(jw, "sum_disk_size", base->sum_disk_size);

	jw_object_inline_begin_object(jw, "count_by_whence");
	{
		/*
		 * Missing is not technically a "whence" value, but
		 * we don't need to clutter up the results with that
		 * distinction.
		 */
		JW_OBJ_INT_NZ(jw, "missing", base->cnt_missing);

		JW_OBJ_INT_NZ(jw, "cached", base->cnt_cached);
		JW_OBJ_INT_NZ(jw, "loose", base->cnt_loose);
		JW_OBJ_INT_NZ(jw, "packed", base->cnt_packed);
		JW_OBJ_INT_NZ(jw, "dbcached", base->cnt_dbcached);
	}
	jw_end(jw);

	write_hbin_json(jw, "dist_by_size", base->size_hbin);
}

static void survey_json(struct json_writer *jw, int pretty)
{
	struct survey_stats_refs *prs = &survey_stats.refs;
	struct survey_stats_commits *psc = &survey_stats.commits;
	struct survey_stats_trees *pst = &survey_stats.trees;
	struct survey_stats_blobs *psb = &survey_stats.blobs;
	int k;

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

		jw_object_inline_begin_object(jw, "commits");
		{
			write_base_object_json(jw, &psc->base);

			jw_object_inline_begin_object(jw, "count_by_nr_parents");
			{
				struct strbuf parent_key = STRBUF_INIT;
				for (k = 0; k < PVEC_LEN; k++)
					if (psc->parent_cnt_pbin[k]) {
						strbuf_reset(&parent_key);
						strbuf_addf(&parent_key, "P%02d", k);
						jw_object_intmax(jw, parent_key.buf, psc->parent_cnt_pbin[k]);
					}
				strbuf_release(&parent_key);
			}
			jw_end(jw);

			if (psc->max_parents) {
				jw_object_inline_begin_object(jw, "most_parents");
				{
					jw_object_intmax(jw, "parents", psc->max_parents);
					jw_object_string(jw, "oid", oid_to_hex(&psc->oid_max_parents));
				}
				jw_end(jw);
			}

			if (psc->size_largest) {
				jw_object_inline_begin_object(jw, "largest_size");
				{
					jw_object_intmax(jw, "size", psc->size_largest);
					/*
					 * TODO Consider only printing OIDs when verbose or
					 * have a PII flag.
					 */
					jw_object_string(jw, "oid", oid_to_hex(&psc->oid_largest));
				}
				jw_end(jw);
			}
		}
		jw_end(jw);

		jw_object_inline_begin_object(jw, "trees");
		{
			write_base_object_json(jw, &pst->base);

			jw_object_intmax(jw, "sum_entries", pst->sum_entries);

			if (pst->max_entries) {
				jw_object_inline_begin_object(jw, "largest_tree");
				{
					jw_object_intmax(jw, "entries", pst->max_entries);
					jw_object_string(jw, "oid", oid_to_hex(&pst->oid_max_entries));
				}
				jw_end(jw);
			}

			write_qbin_json(jw, "dist_by_nr_entries", pst->entry_qbin);
		}
		jw_end(jw);


		jw_object_inline_begin_object(jw, "blobs");
		{
			write_base_object_json(jw, &psb->base);

			if (psb->size_largest) {
				jw_object_inline_begin_object(jw, "largest_size");
				{
					jw_object_intmax(jw, "size", psb->size_largest);
					jw_object_string(jw, "oid", oid_to_hex(&psb->oid_largest));
				}
				jw_end(jw);
			}
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
