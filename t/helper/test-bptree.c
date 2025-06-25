#include "test-tool.h"
#include "btree/btree.h"

static char *__bptree_int(const void *p)
{
	static char buf[32];
	snprintf(buf, sizeof(buf), "%d", (int)(intptr_t)(p));
	return buf;
}

static char *__bptree_str(const void *p)
{
	return (char *)p;
}

static int intcmp(const void *a, const void *b)
{
    return (int)(intptr_t)(a) - (int)(intptr_t)(b);
}

static int print_node(void *key, void *value)
{
	printf("Key: %d, Value: %s\n", (int)(intptr_t)key, (const char *)value);
	return 1;
}

static void *assert_exists(struct bptree *tree, void *key)
{
	void *value = bptree_get(tree, key);
	if (!value)
		die("Key %d should exist", (int)(intptr_t)key);
	return value;
}

static void assert_not_exists(struct bptree *tree, void *key)
{
	if (bptree_exists(tree, key))
		die("Key %d should not exist", (int)(intptr_t)key);
}

static void assert_equal(const char *expected, const char *actual)
{
	if (strcmp(expected, actual) != 0)
		die("Expected '%s', got '%s'", expected, actual);
}

static void assert_size(size_t expected, size_t actual)
{
	if (expected != actual)
		die("Expected size %"PRIuMAX", got %"PRIuMAX, expected, actual);
}

static void run_all(void)
{
	void *found;

	/* Create a B+ tree of order 4 */
	struct bptree *tree = bptree_create(4, intcmp);
	bptree_insert(tree, (void*)10, (void*)"ten");
	bptree_insert(tree, (void*)20, (void*)"twenty");
	bptree_insert(tree, (void*)30, (void*)"thirty");
	bptree_insert(tree, (void*)40, (void*)"fourty");
	bptree_insert(tree, (void*)50, (void*)"fifty");
	bptree_insert(tree, (void*)60, (void*)"sixty");
	bptree_insert(tree, (void*)70, (void*)"seventy");
	__bptree_debug(tree, __bptree_int, __bptree_str);

	/* Check the size of the tree */
	assert_size(7, bptree_size(tree));

	/* Print each key-value pair */
	bptree_foreach(tree, print_node);

	/* Get the value for 60 */
	found = assert_exists(tree, (void*)60);
	assert_equal("sixty", (char*)found);

	/* Try and find non-existant key 42 */
	assert_not_exists(tree, (void*)42);

	/* Insert 42 = fourty-two */
	bptree_insert(tree, (void*)42, (void*)"forty-two");
	__bptree_debug(tree, __bptree_int, __bptree_str);

	/* Check if the key 42 exists */
	assert_exists(tree, (void*)42);

	/* Check the size of the tree */
	assert_size(8, bptree_size(tree));

	/* Print each key-value pair */
	bptree_foreach(tree, print_node);

	/* Remove key 42 */
	bptree_remove(tree, (void*)42);
	__bptree_debug(tree, __bptree_int, __bptree_str);

	/* Check key 42 was removed */
	assert_not_exists(tree, (void*)42);

	/* Check the size of the tree */
	assert_size(7, bptree_size(tree));

	/* Free all resources */
	bptree_release(tree);
}

static const char *const bptree_usage = "\n"
"  test-tool bptree all\n";

int cmd__bptree(int argc, const char **argv)
{
	if (argc < 2)
		usage(bptree_usage);

	if (!strcmp(argv[1], "all")) {
		run_all();
	}

	return 0;
}
