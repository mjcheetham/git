#ifndef BTREE_H
#define BTREE_H

#include <stddef.h>

/*
 * A B+ tree is a self-balancing tree data structure that maintains sorted data
 * and allows searches, sequential access, insertions, and deletions in
 * logarithmic time. Compared to a regular B-tree, a B+ tree has all values at
 * at the leaf level, and internal nodes only store keys to guide searches.
 *
 * A B+ tree is defined by its order, which determines the maximum number of
 * children each node can have. This means the maximum number of keys in each
 * node is order - 1. The B+ tree is balanced, meaning all leaf nodes are at
 * the same depth, and it maintains a linked list of leaf nodes for efficient
 * sequential access.
 *
 * Example B+ Tree (order 4):
 * --------------------------
 *
 *              [    3         5    ]
 *             /          |          \
 *   [  1   2  ] --> [  3   4  ] --> [  5   6   7  ]
 *      |   |           |   |           |   |   |
 *      d1  d2          d3  d4          d5  d6  d7
 *
 *
 * Example API Usage:
 * ------------------
 * #include <stddef.h>
 * #include <stdio.h>
 * #include "btree.h"
 *
 * static int intcmp(const void *a, const void *b)
 * {
 *     return (int)(intptr_t)(a) - (int)(intptr_t)(b);
 * }
 *
 * static int print_node(void *key, void *value)
 * {
 *     printf("Key: %d, Value: %s\n", (int)(intptr_t)key, (const char *)value);
 *     return 1; // Continue traversal
 * }
 *
 * int main(void)
 * {
 *     const char *found;
 *     size_t size;
 *
 *     // Create a B+ tree of order 4
 *     struct btree *tree = btree_create(4, intcmp);
 *     btree_insert(tree, (void*)1, (void*)"one");
 *     btree_insert(tree, (void*)2, (void*)"two");
 *     btree_insert(tree, (void*)3, (void*)"three");
 *     btree_insert(tree, (void*)4, (void*)"four");
 *     btree_insert(tree, (void*)5, (void*)"five");
 *     btree_insert(tree, (void*)6, (void*)"six");
 *     btree_insert(tree, (void*)7, (void*)"seven");
 *
 *     // Print the size of the B+ tree
 *     size = btree_size(tree);
 *     printf("B+ tree size: %"PRIuMAX"\n", size);
 *
 *     // Traverse the B+ tree and print each key-value pair
 *     btree_foreach(tree, print_node);
 *
 *     // Get the value for a key in the B+ tree
 *     found = btree_get(tree, (void*)6);
 *     if (found != NULL) {
 *         printf("Get value for 6 from the B+ tree: %s\n", found);
 *     }
 *
 *     // Try and find a non-existant key
 *     if (btree_exists(tree, (void*)42))
 *         printf("Key 42 exists in the B+ tree.\n");
 *     else
 *         printf("Key 42 does not exist in the B+ tree.\n");
 *
 *     // Remove a key from the B+ tree
 *     btree_remove(tree, (void*)6);
 *
 *     // Free all resources
 *     btree_release(tree);
 *     return 0;
 * }
 *
 * In this example, a B+ tree is created, several keys are inserted, searched,
 * removed and traversed.
 */

struct btree;

/*
 * Comparison function for keys. Should return <0, 0, >0 as per strcmp.
 */
typedef int (*btree_cmp_fn)(const void *a, const void *b);

/*
 * Create a new B+ tree with the given order and comparison function.
 * Returns a pointer to the tree, or NULL on error.
 */
struct btree *btree_create(int order, btree_cmp_fn cmp);

/*
 * Release the B+ tree and free all associated memory.
 */
void btree_release(struct btree *tree);

/*
 * Return the order of the B+ tree.
 */
int btree_order(struct btree *tree);

/*
 * Return the number of key-value pairs in the B+ tree.
 */
size_t btree_size(struct btree *tree);

/*
 * Insert a key-value pair into the B+ tree.
 * Returns 1 on success, 0 if the key already exists or on error.
 */
int btree_insert(struct btree *tree, void *key, void *value);

/*
 * Get the value for a key in the B+ tree.
 * Returns the value if found, or NULL if not found.
 */
void *btree_get(struct btree *tree, void *key);

/*
 * Check if a key exists in the B+ tree.
 * Returns 1 if the key exists, 0 if not found or on error.
 */
int btree_exists(struct btree *tree, void *key);

/*
 * Remove a key from the B+ tree.
 * Returns 1 if the key was deleted, 0 if not found or on error.
 */
int btree_remove(struct btree *tree, void *key);

typedef int (*btree_foreach_fn)(void *key, void *value);

/*
 * Traverse the B+ tree and apply the given function to each key-value pair.
 * The function should return 1 to continue traversal, or zero to stop.
 */
void btree_foreach(struct btree *tree, btree_foreach_fn fn);

/* Function to create a string representation of the B+ tree key or value. */
typedef char *(*__btree_debug_strfn)(const void *p);

/*
 * Debugging function to print the structure of the B+ tree.
 * This is useful for visualizing the tree during development.
 * The keyfn and valfn parameters are functions that convert keys and values
 * to strings for printing. Passing NULL will print the void* keys/values.
 */
void __btree_debug(struct btree *tree, __btree_debug_strfn keyfn,
		    __btree_debug_strfn valfn);

#endif
