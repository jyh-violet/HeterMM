#include <pthread.h>
#include "bltree.h"
pthread_rwlock_t blt_lock = PTHREAD_RWLOCK_INITIALIZER;

int g_order = DEFAULT_TREE_ORDER;

//#define __DEBUG

bl_root_obj *init_bl_tree()
{	
	bl_root_obj *root;

	root = static_cast<bl_root_obj *>(malloc(sizeof(*root)));
	root->b_root = NULL;
#ifdef USE_HMM
    root->hybridMemory = new HybridMemory(MaxThreadNum, 0.8, false, NULL, NULL, NULL);
#endif
//	printf("b link tree of order %d\n", DEFAULT_TREE_ORDER);
	return root;
}

value_t *search_record(bl_root_obj * blRootObj, bl_node *leaf, uint64_t target_key, bool flag)
{
#ifndef B_SEARCH	
	int i;
	static int count;

	for (i = 0; i < leaf->num_keys; i++) {
		if (target_key == leaf->keys[i]) 
			return (char *)leaf->ptrs[i];
	}
	return NULL;
#else
	int start, mid, end;
	static int count;
	start = 0;
	end = leaf->num_keys - 1;
	while (start <= end) {
		mid = start + (end - start) / 2;
		if (leaf->keys[mid] == target_key)
#ifdef USE_HMM
            return get_value_for_read(blRootObj->hybridMemory, *(LogAddress*)(leaf->ptrs + mid));
#else
		    return  (value_t *)&(leaf->ptrs[mid]);
#endif
		if (leaf->keys[mid] < target_key)
			start = mid + 1;
		else
			end = mid - 1;
	}
    vmlog(WARN, "search_record NULL :%ld", target_key);
	return NULL;
#endif
}

int travel_link[64] = {0};
bool check_link = false;
bl_node * travel_link_rlock(bl_node* node, uint64_t target_key){
    bl_node * next = NULL;
    while (node->next != NULL && target_key >= node->nextNodeMin){
#ifdef __DEBUG

        if(check_link){
            travel_link[threadId] ++;
            SegFault
        }
#endif
        pthread_rwlock_rdlock(&node->next->node_lock);
        next = node->next;
        vmlog(LOG, "travel_internal_link node:%p, read lock", &node->next->node_lock);
        pthread_rwlock_unlock(&node->node_lock);
        vmlog(LOG, "travel_internal_link node:%p, unlock", &node->node_lock);
        node = next;
}
return node;
}

bl_node * travel_link_wlock(bl_node* node, uint64_t target_key){
    bl_node * next = NULL;
    while (node->next != NULL && target_key >= node->nextNodeMin){
        pthread_rwlock_wrlock(&node->next->node_lock);
        next = node->next;
        vmlog(LOG, "travel_leaf_link node:%p, write lock", &node->next->node_lock);
        pthread_rwlock_unlock(&node->node_lock);
        vmlog(LOG, "travel_leaf_link node:%p, unlock", &node->node_lock);
        node = next;
    }
    return node;
}

bl_node *get_leaf(bl_node *root, uint64_t target_key)
{
	int i;
	bl_node *node = root, *parent = NULL;

	if (!root)
		return root;
	while (!node->is_leaf) {
        pthread_rwlock_rdlock(&node->node_lock);
        vmlog(LOG, "get_leaf node:%p, read lock", &node->node_lock);

        node = travel_link_rlock(node, target_key);
		i = 0;
#ifdef B_SEARCH
        int start, mid, end;
        static int count;
        start = 0;
        end = node->num_keys - 1;
        while (start <= end) {
            mid = start + (end - start) / 2;
            if (node->keys[mid] == target_key){
                break;
            }

            if (node->keys[mid] < target_key)
                start = mid + 1;
            else
                end = mid - 1;
        }
        if(node->keys[mid] <= target_key){
            i = mid + 1;
        } else {
            i = mid;
        }

#else
        while (i < node->num_keys) {
            if (target_key >= node->keys[i])
                ++i;
            else
                break;
        }
#endif
		parent = node;
		node = (bl_node *)node->ptrs[i];
		pthread_rwlock_unlock(&parent->node_lock);
		vmlog(LOG, "get_leaf node:%p, unlock", &parent->node_lock);

	}
    pthread_rwlock_wrlock(&node->node_lock);
	vmlog(LOG, "get_leaf node:%p, write lock", &node->node_lock);
	return travel_link_wlock(node, target_key);
}

int total_search[64] = {0};
bl_node *get_leaf_read(bl_node *root, uint64_t target_key)
{
    int i;
    bl_node *node = root, *parent = NULL;

    if (!root)
        return root;
    while (!node->is_leaf) {
        pthread_rwlock_rdlock(&node->node_lock);
//        vmlog(LOG, "get_leaf_read node:%p, read lock", &node->node_lock);

        node = travel_link_rlock(node, target_key);
        i = 0;
#ifdef B_SEARCH
        int start, mid, end;
        static int count;
        start = 0;
        end = node->num_keys - 1;
        while (start <= end) {
#ifdef __DEBUG
            total_search[threadId] ++;
#endif
            mid = start + (end - start) / 2;
            if (node->keys[mid] == target_key){
                break;
            }

            if (node->keys[mid] < target_key)
                start = mid + 1;
            else
                end = mid - 1;
        }
        if(node->keys[mid] <= target_key){
            i = mid + 1;
        } else {
            i = mid;
        }

#else
        while (i < node->num_keys) {
            if (target_key >= node->keys[i])
                ++i;
            else
                break;
        }
#endif
        parent = node;
        node = (bl_node *)node->ptrs[i];
        pthread_rwlock_unlock(&parent->node_lock);
//        vmlog(LOG, "get_leaf_read node:%p, unlock", &parent->node_lock);
    }
    pthread_rwlock_rdlock(&node->node_lock);
//    vmlog(LOG, "get_leaf_read node:%p, read lock", &node->node_lock);
//    bl_node * oldnode = node;
    node =  travel_link_rlock(node, target_key);
//    if(oldnode!= node && check_link) {
////        SegFault
//    }
//    if (search_record(node, target_key, NULL) ==NULL){
//        SegFault
//    }
    return node;
}

value_t *bl_tree_read(void *p_root, uint64_t key)
{	
	bl_root_obj *_root;
	bl_node *root, *leaf;
	value_t *val;
	
	_root = (bl_root_obj *)p_root;
	root = _root->b_root;
	if (!root) {
		return NULL;
	}
    if(root->parent != NULL){
        SegFault
    }
	leaf = get_leaf_read(root, key);
	val = search_record(_root, leaf, key, true);
    pthread_rwlock_unlock(&leaf->node_lock);
    vmlog(LOG, "bl_tree_read node:%p, unlock", &leaf->node_lock);
	return val;
/*	if (!val) 
		printf("key %ld not found\n", key);
	else 
		printf("Record at %p -- key %ld, val %s\n", 
				val, key, val);*/
}

int scan_range(bl_node *root, uint64_t start_key,
		int range, uint64_t *buff)
{
	int i = 0, count = 0;
	bl_node *leaf;
	bool finish = false;

	leaf = get_leaf(root, start_key);
	if (!leaf)
		return 0;
	for (i = 0; i < leaf->num_keys; ++i) {
		if (leaf->keys[i] >= start_key)
			break;
	}
	if (i == leaf->num_keys)
		return 0;
	while (leaf) {
		for (; i < leaf->num_keys; ++i) {

			/*if (leaf->keys[i] <= end_key)
				++count;*/
			if (count <= range) {
				buff[i] = leaf->keys[i];
				++count;
			}
			else {
				finish = true;
				break;
			}
		}
		if (finish)
			break;
		leaf = static_cast<bl_node *>(leaf->ptrs[g_order - 1]);
		i = 0;
	}
	return count;
}

int bl_tree_scan(void *p_root, uint64_t start_key, int range,
		uint64_t *buff)
{
	bl_root_obj *_root;
	bl_node *root;
	int key_count;

	_root = (bl_root_obj *)p_root;
	root = _root->b_root;
	key_count = scan_range(root, start_key, 
			range, buff);
	return key_count;
}

bl_node *create_node()
{
	bl_node *new_node;

	new_node = static_cast<bl_node *>(malloc(sizeof(*new_node)));
	if (!new_node) {
		perror("bpt node create failed");
		exit(0);
	}
	new_node->is_leaf = false;
	new_node->num_keys = 0;
	new_node->parent = new_node->next = NULL;
	new_node->next = NULL;
    new_node->nextNodeMin = 0xFFFFFFFFFFFFFFFF;
	new_node->node_lock = PTHREAD_RWLOCK_INITIALIZER;
	return new_node;
}

bl_node *create_leaf()
{
	bl_node *leaf;

	leaf = create_node();
	leaf->is_leaf = true;
	return leaf;
}

int cut(int len)
{
	if (len % 2 == 0)
		return len/2;
	else
		return len/2 + 1;
}

bl_node *create_bl_tree(uint64_t key, bl_val_t rec)
{
	bl_node *root;
#ifndef USE_HMM
    value_t* v = static_cast<value_t *>(malloc(sizeof(value_t)));
    *(bl_val_t*)v = rec;
#endif
	root = create_leaf();
	root->keys[0] = key;
#ifdef USE_HMM
    root->ptrs[0] = (void *)rec;
#else
	root->ptrs[0] = v;
#endif
	root->ptrs[g_order - 1] = NULL;
	root->parent = NULL;
	++root->num_keys;
	return root;
}

bl_node *insert_into_leaf(bl_node *leaf, uint64_t key,
                         bl_val_t rec)
{
    value_t* v = static_cast<value_t *>(malloc(sizeof(value_t)));
    *(bl_val_t*)v = rec;
	int i; 
	int slot = 0;
	int n_keys = leaf->num_keys;
	
	while (slot < n_keys && leaf->keys[slot] < key) 
		++slot;
	for (i = n_keys; i > slot; --i) {
		leaf->keys[i] = leaf->keys[i - 1];
		leaf->ptrs[i] = leaf->ptrs[i - 1];
	}
	leaf->keys[slot] = key;
	leaf->ptrs[slot] = v;
	++leaf->num_keys;
    vmlog(LOG, "insert_into_leaf key:%ld, leaf:%p", key, (void *)leaf);
#ifdef __DEBUG
    for (int j = 0; j < leaf->num_keys - 1; ++j) {
        if(leaf->keys[j] >= leaf->keys[j + 1]){
            SegFault
        }
    }

    if(leaf->next!= NULL){
        if(key >= leaf->next->keys[0] || leaf->keys[leaf->num_keys -1] >= leaf->next->keys[0]){
            SegFault
        }
    }
#endif
	return leaf;
}

bool update_leaf(bl_root_obj* blRootObj,  bl_node *leaf, uint64_t key,
                 value_t& rec){
    int slot = 0;

#ifdef B_SEARCH
    int start, mid, end;
    static int count;
    start = 0;
    end = leaf->num_keys - 1;
    while (start <= end) {
        mid = start + (end - start) / 2;
        if (leaf->keys[mid] == key){
            slot = mid;
            break;
        }

        if (leaf->keys[mid] < key)
            start = mid + 1;
        else
            end = mid - 1;
    }
#else
    int n_keys = leaf->num_keys;
    while (slot < n_keys && leaf->keys[slot] < key)
        ++slot;
#endif
    if(leaf->keys[slot] == key){
#ifdef USE_HMM
        set_value(blRootObj->hybridMemory, *(LogAddress*)(leaf->ptrs+slot), key, &rec);
#else
        memcpy(leaf->ptrs[slot], (const void *) &rec, sizeof (rec));
        vmlog(LOG, "update_leaf key:%ld, leaf:%p", key, (void *)leaf);
#endif
        return true;
    }
    return false;
}

bl_node *insert_into_new_root(bl_node *left, uint64_t key,
		bl_node *right)
{
	bl_node *root;
	
	/* this new node will be in the allocator
	 * list and will be flushed at the 
	 * undolog commit*/
	root = create_node();
	root->keys[0] = key;
	root->ptrs[0] = left;
	root->ptrs[1] = right;
	++root->num_keys;
	root->parent = NULL;
	left->parent = root;
	right->parent = root;
	pthread_rwlock_unlock(&left->node_lock);
	pthread_rwlock_unlock(&right->node_lock);
	vmlog(LOG, "insert_into_new_root: new root:%p, left:%p unlock", &root->node_lock, &left->node_lock);
    vmlog(LOG, "insert_into_new_root: new root:%p, left:%p, right:%p", (void *)root, root->ptrs[0], root->ptrs[1]);
	return root;
}

int get_left_index(bl_node *parent, bl_node *left)
{
	int left_index = 0;

	while(left_index <= parent->num_keys &&
			parent->ptrs[left_index] != left) {
		++left_index;
	}
	return left_index;
}

bl_node *insert_into_node(bl_node *root, bl_node *n,
		int left_index, uint64_t key, bl_node *right)
{
	int i;

	for (i = n->num_keys; i > left_index; --i) {
		/* node n is already backedup in 
		 * the undolog or it is present in 
		 * the allocator list*/
		n->ptrs[i + 1] = n->ptrs[i];
		n->keys[i] = n->keys[i - 1];
	}
	n->ptrs[left_index + 1] = right;
	n->keys[left_index] = key;
	++n->num_keys;
#ifdef __DEBUG
    vmlog(LOG, "insert_into_node:%d right:%p, key:%ld, left_index:%d, n:%p, num_keys:%d",
          __LINE__,(void *)right, key, left_index, (void *)n, n->num_keys);
    for (int j = 0; j <= n->num_keys; ++j) {
        if(n->ptrs[j] == NULL){
            SegFault
        }
    }
    for (int j = 0; j < n->num_keys - 1; ++j) {
        if(n->keys[j] >= n->keys[j + 1]){
            SegFault
        }
    }
    if(n->next!= NULL){
        if(key >= n->next->keys[0] || n->keys[n->num_keys -1] >= n->next->keys[0]){
            SegFault
        }
    }
#endif
	return root;
}

bl_node *split_insert_node(bl_node *root, bl_node *old_node,
		int left_index, uint64_t key, bl_node *right)
{
	int i, j, split;
	uint64_t k_prime;
	bl_node *new_node, *child;
	uint64_t *temp_keys;
	bl_node **temp_ptrs;

	temp_keys = (uint64_t *)malloc((g_order + 2) * sizeof(uint64_t));
	if (!temp_keys) {
		perror("node split failed");
		exit(0);
	}
	temp_ptrs = (bl_node **)malloc((g_order + 2) * sizeof(bl_node *));
	if (!temp_ptrs) {
		perror("node split failed");
		exit(0);
	}
	for (i = 0, j = 0; i < old_node->num_keys + 1; ++i, ++j) {
		if (j == left_index + 1)
			++j;
		temp_ptrs[j] = static_cast<bl_node *>(old_node->ptrs[i]);
	}
	for (i = 0, j = 0; i < old_node->num_keys; ++i, ++j) {
		if (j == left_index)
			++j;
		temp_keys[j] = old_node->keys[i];
	}
	temp_ptrs[left_index + 1] = right;
	temp_keys[left_index] = key;
	split = cut(g_order);
    int old_num = old_node->num_keys;
	/* new_node will be in the 
	 * allocator list*/
	new_node = create_node();
    pthread_rwlock_wrlock(&new_node->node_lock);
#ifdef __DEBUG
    for (int k = 0; k <= g_order; ++k) {
        if(temp_ptrs[k] == NULL){
            SegFault
        } else{
            vmlog(LOG, "split_insert_node temp_ptrs[%d]:%p", k, (void *)temp_ptrs[k]);
        }
    }
#endif
    vmlog(LOG, "split_insert_node:node:%p write lock, old_node->num_keys:%d", &new_node->node_lock, old_node->num_keys);
	old_node->num_keys = 0;
	for (i = 0; i < split - 1; ++i) {
		/* old_node is already backed up 
		 * in the undolog*/
		old_node->ptrs[i] = temp_ptrs[i];
		old_node->keys[i] = temp_keys[i];
		++old_node->num_keys;
	}

	old_node->ptrs[i] = temp_ptrs[i];
	k_prime = temp_keys[i];
    old_node->keys[i] = 0;
#ifdef __DEBUG
    for (int k = 0; k <= old_node->num_keys; ++k) {
        if(old_node->ptrs[k] == NULL){
            SegFault
        }
    }
    vmlog(LOG, "split_insert_node old_node:%p, num_keys:%d, i:%d, split:%d",
          (void *)old_node, old_node->num_keys,i, split);
#endif
	for (++i, j = 0; i < g_order; ++i, ++j) {
		new_node->ptrs[j] = temp_ptrs[i];
        temp_ptrs[i]->parent = new_node;
		new_node->keys[j] = temp_keys[i];
		++new_node->num_keys;
        vmlog(LOG, "split_insert_node temp_ptrs:%p, parent:%p", (void *)temp_ptrs[i], (void *)temp_ptrs[i]->parent);
	}

	new_node->ptrs[j] = temp_ptrs[i];
    temp_ptrs[i]->parent = new_node;
    new_node->keys[j] = 0;

    for (int k = old_node->num_keys+1; k <= g_order; ++k) {
        old_node->ptrs[k] = NULL;
        old_node->keys[k] = 0;
    }
    for (int k = new_node->num_keys + 1; k <= g_order; ++k) {
        new_node->ptrs[k] = NULL;
        new_node->keys[k] = 0;
    }
#ifdef __DEBUG
    for (int k = 0; k <= new_node->num_keys; ++k) {
        if(new_node->ptrs[k] == NULL){
            SegFault
        }
    }
    for (int k = 0; k <= old_node->num_keys; ++k) {
        if(old_node->ptrs[k] == NULL){
            SegFault
        }
    }
    vmlog(LOG, "split_insert_node temp_ptrs:%p, parent:%p", (void *)temp_ptrs[i], (void *)temp_ptrs[i]->parent);
    vmlog(LOG, "split_insert_node old node:%p, num:%d, new node:%p, num:%d, left_index:%d",
          (void *)old_node, old_node->num_keys, (void *)new_node, new_node->num_keys, left_index);
#endif
    free(temp_ptrs);
	free(temp_keys);
	new_node->parent = old_node->parent;
	new_node->next = old_node->next;
	old_node->next = new_node;
	new_node->nextNodeMin = old_node->nextNodeMin;
	old_node->nextNodeMin = k_prime;
    pthread_rwlock_unlock(&right->node_lock);
    vmlog(LOG, "split_insert_node:node:%p unlock", &right->node_lock);
	return bl_insert_into_parent(root, old_node,
			k_prime, new_node);
}

bl_node *bl_insert_into_parent(bl_node *root, bl_node *left,
		uint64_t key, bl_node *right)
{
	int left_index;
	bl_node *parent;
	int ret;
retry:
	parent = left->parent;
	if (!parent)
		/* the new parent will be in the 
		 * allocator list*/
		return insert_into_new_root(left, key, right);


	pthread_rwlock_wrlock(&parent->node_lock);
	vmlog(LOG, "bl_insert_into_parent node:%p, write lock", &parent->node_lock);
	left_index = get_left_index(parent, left);
	if(left_index >parent->num_keys){
        pthread_rwlock_unlock(&parent->node_lock);
        vmlog(LOG, "bl_insert_into_parent:%d node:%p, unlock, left_index:%d, MAX_KEYS:%d",
              __LINE__, &parent->node_lock, left_index, g_order);
        goto retry;
	}
	right->parent = parent;
    vmlog(LOG, "bl_insert_into_parent right:%p, parent:%p", (void *)right, (void *)right->parent);

    if (parent->num_keys < MAX_KEYS){
	    insert_into_node(root, parent,
                         left_index, key, right);
        pthread_rwlock_unlock(&parent->node_lock);
        pthread_rwlock_unlock(&left->node_lock);
        pthread_rwlock_unlock(&right->node_lock);
        vmlog(LOG, "bl_insert_into_parent node:%p, unlock", &parent->node_lock);
        vmlog(LOG, "bl_insert_into_parent node:%p, unlock", &left->node_lock);
        vmlog(LOG, "bl_insert_into_parent node:%p, unlock", &right->node_lock);
        return root;
	}
	pthread_rwlock_unlock(&left->node_lock);
	vmlog(LOG, "bl_insert_into_parent node:%p, unlock", &left->node_lock);
	return split_insert_node(root, parent, 
			left_index, key, right);
}

bl_node *split_insert_into_leaf(bl_node *root, bl_node *leaf,
                               uint64_t key, bl_val_t rec)
{
#ifndef USE_HMM
    value_t* v = static_cast<value_t *>(malloc(sizeof(value_t)));
    *(bl_val_t*)v = rec;
#endif

    bl_node *new_leaf;
	uint64_t *temp_keys;
	void **temp_ptrs;
	int slot = 0;
	int split, i, j;
	uint64_t new_key;
	
	/* this new leaf will be in the allocator list
	 * and will be flushed at the end of the undolog
	 * commit*/
	new_leaf = create_leaf();
    pthread_rwlock_wrlock(&new_leaf->node_lock);
    vmlog(LOG, "split_insert_into_leaf node:%p, write lock", &new_leaf->node_lock);
	temp_keys = static_cast<uint64_t *>(malloc((g_order + 1) * sizeof(uint64_t)));
	if (!temp_keys) {
		perror("leaf split failed");
		exit(0);
	}
	temp_ptrs = static_cast<void **>(malloc((g_order + 1) * sizeof(void *)));
	if (!temp_ptrs) {
		perror("leaf split failed");
		exit(0);
	}
	while (slot < g_order && leaf->keys[slot] < key)
		++slot;
	for (i = 0, j = 0; i < leaf->num_keys; ++i, ++j) {
		if (j == slot)  
			++j;
		temp_keys[j] = leaf->keys[i];
		temp_ptrs[j] = leaf->ptrs[i];
	}
	temp_keys[slot] = key;
#ifdef USE_HMM
    temp_ptrs[slot] = (void *)rec;
#else
	temp_ptrs[slot] = v;
#endif
    vmlog(LOG, "split_insert_into_leaf key:%ld, leaf:%p", key, (void *)leaf);
	leaf->num_keys = 0;
	split = cut(g_order);
	for (i = 0; i < split; ++i) {
		/* the old leaf has been already backedup
		 * in the undolog or it should be in the
		 * allocator list*/
		leaf->ptrs[i] = temp_ptrs[i];
		leaf->keys[i] = temp_keys[i];
		++leaf->num_keys;
	}
	for (i = split, j = 0; i <= g_order; ++i, ++j) {
		new_leaf->keys[j] = temp_keys[i];
		new_leaf->ptrs[j] = temp_ptrs[i];
		++new_leaf->num_keys;
	}
	free(temp_keys);
	free(temp_ptrs);
	new_leaf->next = leaf->next;
	leaf->next = new_leaf;
	for (i = leaf->num_keys; i < g_order; ++i) {
		leaf->ptrs[i] = NULL;
        leaf->keys[i] = 0;
	}
	for (i = new_leaf->num_keys; i < g_order; ++i) {
		new_leaf->ptrs[i] = NULL;
        new_leaf->keys[i] = 0;
	}
	new_leaf->parent = leaf->parent;
	new_key = new_leaf->keys[0];
	new_leaf->nextNodeMin = leaf->nextNodeMin;
	leaf->nextNodeMin = new_key;
    vmlog(LOG, "split_insert_into_leaf new_leaf:%p, parent:%p, new_key:%ld, leaf:%p",
          (void *)new_leaf, (void *)new_leaf->parent, new_key, (void *)leaf);
	return bl_insert_into_parent(root, leaf,
			new_key, new_leaf);
}

int bl_tree_insert(void *p_root, uint64_t key,
                   value_t val)
{
	bl_node *leaf = NULL;
	bl_root_obj *_root;
	bl_node **root;
	bl_node *temp;

	_root = (bl_root_obj *)p_root;

    pthread_rwlock_wrlock(&blt_lock);
    root = &_root->b_root;

	/* create a new tree when root is empty*/
	if (!(*root)) {
#ifdef USE_HMM
        bl_val_t addr = AddressNil;
        set_value(_root->hybridMemory, addr, key, &val);
        *root = create_bl_tree(key, addr);
#else
        *root = create_bl_tree(key, val);
#endif
	    pthread_rwlock_unlock(&blt_lock);
		return 0;
	}
    while (((*root)->parent != NULL)){
        *root = (*root)->parent;
    }
    pthread_rwlock_unlock(&blt_lock);
	
	leaf = get_leaf(*root, key);

	if(update_leaf(_root, leaf, key, val)){
	    pthread_rwlock_unlock(&leaf->node_lock);
        vmlog(LOG, "bl_tree_insert:%d node:%p, unlock", __LINE__, &leaf->node_lock);
	    return 0;
	}

#ifdef USE_HMM
    bl_val_t addr = AddressNil;
    set_value(_root->hybridMemory, addr, key, &val);
#endif
	/* try inserting new record in the leaf*/
	if (leaf->num_keys < g_order) {
#ifdef USE_HMM
        leaf = insert_into_leaf(leaf, key, addr);
#else
        leaf = insert_into_leaf(leaf, key, val);
#endif
        pthread_rwlock_unlock(&leaf->node_lock);
        vmlog(LOG, "bl_tree_insert:%d node:%p, unlock", __LINE__, &leaf->node_lock);
		return 0;
	}
#ifdef USE_HMM
    temp = split_insert_into_leaf(*root, leaf,
                                  key, addr);
#else
	temp = split_insert_into_leaf(*root, leaf, 
			key, val);
#endif
    pthread_rwlock_wrlock(&blt_lock);

    if (temp != *root) {
		/* bptree root has changed
		 * update the root obj*/
		*root = temp;
	}
    pthread_rwlock_unlock(&blt_lock);
	return 0;
}