#ifndef GVFS_H
#define GVFS_H


/*
 * This file is for the specific settings and methods
 * used for GVFS functionality
 */


/*
 * The list of bits in the core_gvfs setting
 */
#define GVFS_SKIP_SHA_ON_INDEX                      (1 << 0)
#define GVFS_MISSING_OK                             (1 << 2)
#define GVFS_NO_DELETE_OUTSIDE_SPARSECHECKOUT       (1 << 3)

void gvfs_load_config_value(const char *value);
int gvfs_config_is_set(int mask);

#endif /* GVFS_H */
