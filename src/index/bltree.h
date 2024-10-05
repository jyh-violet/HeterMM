#ifndef _BLTREE_H
#define _BLTREE_H

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include "../hm.hpp"

#ifdef __cplusplus
extern "C" {
#endif

//#define DEFAULT_TREE_ORDER 30 
//#define DEFAULT_TREE_ORDER
// 256
#define DEFAULT_TREE_ORDER 129
#define MAX_KEYS DEFAULT_TREE_ORDER - 1
#define DEFAULT_VAL_LEN 256
#define B_SEARCH

#ifdef USE_HMM
typedef LogAddress bl_val_t;
#else
typedef volatile value_t bl_val_t;
#endif


typedef struct bl_tree_node
{
	uint64_t keys[DEFAULT_TREE_ORDER + 1];
	void *ptrs[DEFAULT_TREE_ORDER + 1];
	struct bl_tree_node *parent;
	bool is_leaf;
	int num_keys;
	struct bl_tree_node *next;
	uint64_t nextNodeMin;
	pthread_rwlock_t node_lock;
}bl_node;

typedef struct bl_tree_root_obj
{
	bl_node *b_root;
#ifdef USE_HMM
    HybridMemory*     hybridMemory;
#endif
}bl_root_obj;

bl_root_obj *init_bl_tree();
int bl_tree_insert(void *, uint64_t, value_t);
value_t *bl_tree_read(void *, uint64_t);
int bl_tree_scan(void *, uint64_t, int, uint64_t *);
bl_node *bl_insert_into_parent(bl_node *, bl_node *,
		uint64_t, bl_node *);



#ifdef __cplusplus
}
#endif

#endif /* bltree.h*/
