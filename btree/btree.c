#include "git-compat-util.h"
#include "btree.h"
#include <stdlib.h>
#include <math.h>

struct btree_node {
	int nr_keys; /* max nr_keys is always order - 1 */
	unsigned is_leaf : 1;
	void **keys;
	union {
		void *value; /* leaf node */
		struct btree_node *node; /* internal node */
	} *children;
	struct btree_node *next; /* leaf node chaining */
};

struct btree {
	struct btree_node *root;
	int order; /* max number of children per node */
	btree_cmp_fn cmp; /* comparison function for keys */
	size_t size; /* number of leaf values in the tree */
};

static struct btree_node *node_create(int order, int is_leaf)
{
	struct btree_node *node = calloc(1, sizeof(struct btree_node));
	if (!node)
		return NULL;

	node->nr_keys = 0;
	node->keys = calloc(order - 1, sizeof(void *));
	node->next = NULL;
	node->is_leaf = is_leaf ? 1 : 0;
	if (is_leaf)
		node->children = calloc(order - 1, sizeof(void *));
	else
		node->children = calloc(order, sizeof(struct btree_node *));

	return node;
}

static void node_release(struct btree_node *node, int order)
{
	if (!node)
		return;

	free(node->keys);

	/* If this is an internal node, recursively release child nodes */
	if (!node->is_leaf) {
		for (int i = 0; i < order; i++) {
			node_release(node->children[i].node, order);
		}
	}

	free(node->children);
	free(node);
}

struct btree *btree_create(int order, btree_cmp_fn cmp)
{
	struct btree *tree;

	if (order < 3 || !cmp)
		return NULL;

	tree = calloc(1, sizeof(struct btree));
	if (!tree)
		return NULL;

	tree->root = node_create(order, 1);
	tree->order = order;
	tree->cmp = cmp;
	tree->size = 0;

	return tree;
}

void btree_release(struct btree *tree)
{
	if (!tree)
		return;

	node_release(tree->root, tree->order);
	free(tree);
}

int btree_order(struct btree *tree)
{
	return tree ? tree->order : 0;
}

size_t btree_size(struct btree *tree)
{
	return tree ? tree->size : 0;
}

/*
 * Insert key and value into a leaf node at the given index.
 * Returns 1 on success, 0 if key already exists.
 */
static int insert_into_leaf(struct btree_node *leaf, btree_cmp_fn cmp,
			    void *key, void *value, int index)
{
	int i;

	/* Check for duplicate key */
	if (index < leaf->nr_keys && cmp(key, leaf->keys[index]) == 0)
		return 0;

	/* Shift keys and values to make space */
	for (i = leaf->nr_keys; i > index; i--) {
		leaf->keys[i] = leaf->keys[i-1];
		leaf->children[i].value = leaf->children[i-1].value;
	}

	leaf->keys[index] = key;
	leaf->children[index].value = value;
	leaf->nr_keys++;
	return 1;
}

/*
 * Split a leaf node. Returns the new right node.
 * The parent will need to be updated with the new key and child.
 */
static struct btree_node *split_leaf(struct btree_node *leaf, int order,
				     void **out_new_key)
{
	int i;
	int mid = (int)ceil((order - 1) / 2.0);
	struct btree_node *new_leaf = node_create(order, 1);

	/* Move the upper half of keys/values to the new leaf */
	for (i = mid; i < order - 1; i++) {
		new_leaf->keys[i - mid] = leaf->keys[i];
		new_leaf->children[i - mid].value = leaf->children[i].value;
		new_leaf->nr_keys++;
	}

	leaf->nr_keys = mid;

	/* Chain the new leaf into the list */
	new_leaf->next = leaf->next;
	leaf->next = new_leaf;

	/* The new key to push up is the first key in the new leaf */
	if (out_new_key)
		*out_new_key = new_leaf->keys[0];

	return new_leaf;
}

/*
 * Helper to insert a key and child into an internal node at the given index.
 */
static void insert_into_internal(struct btree_node *node, void *key,
				 struct btree_node *right_child, int index)
{
	for (int i = node->nr_keys; i > index; i--) {
		node->keys[i] = node->keys[i-1];
		node->children[i + 1].node = node->children[i].node;
	}

	node->keys[index] = key;
	node->children[index + 1].node = right_child;
	node->nr_keys++;
}

/*
 * Helper to split an internal node. Returns the new right node and sets
 * *out_up_key to the key to push up.
 */
static struct btree_node *split_internal(struct btree_node *node, int order,
					 void **out_up_key)
{
	int i;
	int mid = (int)floor((order - 1) / 2.0);
	struct btree_node *new_node = node_create(order, 0);

	/* The key to push up is the middle key */
	if (out_up_key)
		*out_up_key = node->keys[mid];

	/* Move keys and children after mid to new_node */
	for (i = mid + 1; i < order - 1; i++) {
		new_node->keys[i - (mid + 1)] = node->keys[i];
		new_node->children[i - (mid + 1)].node = node->children[i].node;
		new_node->nr_keys++;
	}

	/* Move the last child pointer */
	new_node->children[new_node->nr_keys].node =
		node->children[order - 1].node;

	/* Shrink the left node */
	node->nr_keys = mid;

	return new_node;
}

/*
 * Insert a key/value into the subtree rooted at node.
 * If a split occurs, sets *out_new_key and *out_new_child for the parent to
 * handle.
 * Returns 1 on success, 0 if key exists.
 */
static int insert_recursive(struct btree_node *node, int order,
			    btree_cmp_fn cmp, void *key, void *value,
			    void **out_new_key,
			    struct btree_node **out_new_child)
{
	int i;
	if (node->is_leaf) {
		struct btree_node *new_leaf;

		/* Find index to insert */
		for (i = 0; i < node->nr_keys; i++) {
			if (cmp(key, node->keys[i]) <= 0)
				break;
		}

		/* If node is full, split before insert (preemptive split) */
		if (node->nr_keys == order - 1) {
			void *split_key = NULL;
			new_leaf = split_leaf(node, order, &split_key);

			/* Decide which node to insert into */
			if (cmp(key, split_key) < 0) {
				if (!insert_into_leaf(node, cmp, key, value, i))
					return 0; /* Duplicate key */
			} else {
				int new_index = i - node->nr_keys;
				if (new_index < 0)
					new_index = 0;

				if (!insert_into_leaf(new_leaf, cmp, key, value,
						      new_index))
					return 0; /* Duplicate key */
			}
			if (out_new_key)
				*out_new_key = split_key;

			if (out_new_child)
				*out_new_child = new_leaf;

			return 1;
		}

		/* If node is not full, insert directly */
		if (!insert_into_leaf(node, cmp, key, value, i))
			return 0; /* Duplicate key */

		return 1;
	} else {
		void *child_new_key = NULL;
		struct btree_node *child_new_child = NULL;
		struct btree_node *new_node;
		int res;

		/* Find child to descend */
		for (i = 0; i < node->nr_keys; i++) {
			if (cmp(key, node->keys[i]) < 0)
				break;
		}

		/* If child is full, split before descent (preemptive split) */
		if (node->children[i].node &&
		    node->children[i].node->nr_keys == order - 1) {
			void *split_key = NULL;
			struct btree_node *split_node = NULL;

			if (node->children[i].node->is_leaf)
				split_node = split_leaf(node->children[i].node,
							order, &split_key);
			else
				split_node = split_internal(
					node->children[i].node, order,
					&split_key);

			insert_into_internal(node, split_key, split_node, i);

			/* Decide which child to descend into */
			if (cmp(key, split_key) < 0)
				; /* Descend into left */
			else
				i++; /* Descend into right */
		}

		res = insert_recursive(node->children[i].node, order, cmp, key,
				       value, &child_new_key, &child_new_child);
		if (!res)
			return 0; /* Duplicate key */

		if (!child_new_child)
			return 1; /* No split below */

		/* Insert new key and child into this node */
		insert_into_internal(node, child_new_key, child_new_child, i);
		if (node->nr_keys == order - 1) {
			/* Split internal node */
			new_node = split_internal(node, order, out_new_key);
			if (out_new_child)
				*out_new_child = new_node;

			return 1;
		}
		return 1;
	}
}

/*
 * Find the leaf node containing the key.
 * If key_idx is not NULL, it will be set to the index of the key in the leaf
 * node if found, or -1 if not found.
 * Returns the leaf node if found, NULL if not found.
 */
static struct btree_node *find_leaf(struct btree_node *node,
				    btree_cmp_fn cmp, void *key, int *key_idx)
{
	int i;

	if (!node)
		return NULL;

	/* Traverse down the tree to find the leaf node */
	while (node) {
		/* Search in the current node */
		for (i = 0; i < node->nr_keys; i++) {
			int cmp_result = cmp(key, node->keys[i]);
			if (cmp_result == 0) {
				/* Key found */
				if (key_idx)
					*key_idx = i;
				return node; /* Found in this leaf */
			} else if (cmp_result < 0) {
				break; /* Go to next child */
			}
		}

		if (node->is_leaf) {
			return NULL; /* Key not found in this leaf */
		} else {
			node = node->children[i].node; /* Move to next child */
		}
	}

	/* Key not found */
	if (key_idx)
		*key_idx = -1;
	return NULL;
}

/*
 * Insert a key-value pair into the B+ tree.
 * Returns 1 on success, 0 if the key already exists or on error.
 */
int btree_insert(struct btree *tree, void *key, void *value)
{
	void *new_key = NULL;
	struct btree_node *new_child = NULL;
	int res;

	if (!tree || !tree->root || !key)
		return 0;

	res = insert_recursive(tree->root, tree->order, tree->cmp, key, value,
			       &new_key, &new_child);
	if (!res)
		return 0; /* Duplicate key or error */

	if (new_child) {
		/* Root was split, create new root */
		struct btree_node *new_root = node_create(tree->order, 0);
		new_root->keys[0] = new_key;
		new_root->children[0].node = tree->root;
		new_root->children[1].node = new_child;
		new_root->nr_keys = 1;
		tree->root = new_root;
	}

	tree->size++;
	return 1;
}

void *btree_get(struct btree *tree, void *key)
{
	struct btree_node *node;
	int key_idx;

	if (!tree || !tree->root || !key)
		return NULL;

	/* Find the leaf node containing the key */
	node = find_leaf(tree->root, tree->cmp, key, &key_idx);
	if (!node || key_idx < 0)
		return NULL; /* Key not found */

	/* Return the value associated with the key */
	return node->children[key_idx].value;
}

int btree_exists(struct btree *tree, void *key)
{
	if (!tree || !tree->root || !key)
		return 0;

	/* Find the leaf node containing the key */
	if (!find_leaf(tree->root, tree->cmp, key, NULL))
		return 0; /* Key not found */

	/* Key exists if we found it in the leaf */
	return 1;
}

int btree_remove(struct btree *tree, void *key)
{
	struct btree_node *leaf;
	int key_idx;

	if (!tree || !tree->root || !key)
		return 0;

	leaf = find_leaf(tree->root, tree->cmp, key, &key_idx);
	if (!leaf || key_idx < 0)
		return 0; /* Key not found */

	/* Remove key and value from leaf */
	for (int i = key_idx; i < leaf->nr_keys - 1; i++) {
		leaf->keys[i] = leaf->keys[i + 1];
		leaf->children[i].value = leaf->children[i + 1].value;
	}
	leaf->nr_keys--;
	tree->size--;

	/*
	 * TODO: Handle underflow and rebalancing (borrow/merge)
	 * For now, this only removes from the leaf and does not rebalance.
	 */

	/* If the root is empty and not a leaf, update root */
	if (tree->root->nr_keys == 0 && !tree->root->is_leaf) {
		struct btree_node *old_root = tree->root;
		tree->root = tree->root->children[0].node;
		free(old_root->keys);
		free(old_root->children);
		free(old_root);
	}

	return 1;
}

void btree_foreach(struct btree *tree, btree_foreach_fn fn)
{
	struct btree_node *node;

	if (!tree || !tree->root || !fn)
		return;

	node = tree->root;

	while (node) {
		int i;

		/* Traverse the leaf nodes */
		if (node->is_leaf) {
			for (i = 0; i < node->nr_keys; i++) {
				if (!fn(node->keys[i], node->children[i].value))
					return;
			}
			node = node->next; /* Move to next leaf node */
		} else {
			/* For internal nodes, go to the first child */
			node = node->children[0].node;
		}
	}
}

#include <stdio.h>

static inline void __print_indent(int level)
{
	for (int i = 0; i < level; i++)
		printf("  ");
}

static void __btree_debug_node(struct btree_node *node,
			       __btree_debug_strfn keyfn,
			       __btree_debug_strfn valfn, int level)
{
	if (!node)
		return;

	/*
	 * For an internal node, print:
	 *
	 * Node (keys: <n>) <node_address>
	 *   Key[0]: <key0>
	 *   Key[1]: <key1>
	 *   ...
	 *   Key[n]: <keyn>
	 *   Child[0]:
	 *     <ChildNode[0]>
	 *   Child[1]:
	 *     <ChildNode[1]>
	 *   ...
	 *   Child[n]:
	 *     <ChildNode[n]>
	 *   Child[n+1]:
	 *     <ChildNode[n+1]>
	 *
	 * For a leaf node, print:
	 *
	 * Leaf (keys: <n>) <leaf_node_address>
	 *   Key[0]: <key0> = <value0>
	 *   Key[1]: <key1> = <value1>
	 *   ...
	 *   Key[n]: <keyn> = <valuen>
	 *  Next: <next_leaf_node_address>
	 *
	 */

	__print_indent(level);
	if (node->is_leaf) {
		printf("Leaf (keys: %d) 0x%p\n", node->nr_keys, (void *)node);

		/* Print all key-value pairs */
		for (int i = 0; i < node->nr_keys; i++) {
			__print_indent(level + 1);
			printf("Key[%d]: ", i);

			if (keyfn)
				printf("%s = ", keyfn(node->keys[i]));
			else
				printf("0x%p = ", node->keys[i]);

			if (valfn)
				printf("%s\n", valfn(node->children[i].value));
			else
				printf("0x%p\n", node->children[i].value);
		}

		__print_indent(level + 1);
		printf("Next: 0x%p\n", (void *)node->next);
	} else {
		printf("Node (keys: %d) 0x%p\n", node->nr_keys, (void *)node);

		/* Print all keys */
		for (int i = 0; i < node->nr_keys; i++) {
			__print_indent(level + 1);
			if (keyfn)
				printf("Key[%d]: %s\n", i,
				       keyfn(node->keys[i]));
			else
				printf("Key[%d]: 0x%p\n", i, node->keys[i]);
		}

		/* Recursively print all child nodes */
		for (int i = 0; i <= node->nr_keys; i++) {
			if (node->children[i].node) {
				__print_indent(level + 1);
				printf("Child[%d]:\n", i);
				__btree_debug_node(node->children[i].node,
						   keyfn, valfn, level + 2);
			}
		}
	}
}

void __btree_debug(struct btree *tree, __btree_debug_strfn keyfn,
		    __btree_debug_strfn valfn)
{
	if (!tree)
		return;

	printf("B+ Tree (order: %d, size: %"PRIuMAX") 0x%p\n", tree->order,
	       tree->size, (void *)tree);

	if (tree->root)
		__btree_debug_node(tree->root, keyfn, valfn, 0);
}
