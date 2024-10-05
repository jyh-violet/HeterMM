
#include "utree.h"
pthread_mutex_t print_mtx;


/*
 * class btree
 */
btree::btree(){
    root = (char*)new page();
    height = 1;
}


void btree::setNewRoot(char *new_root) {
    this->root = (char*)new_root;
    ++height;
}

char *btree::btree_search_pred(entry_key_t key, bool *f, char **prev, bool debug=false){
    page* p = (page*)root;

    while(p->hdr.leftmost_ptr != NULL) {
        p = (page *)p->linear_search(key);
    }

    page *t;
    while((t = (page *)p->linear_search_pred(key, prev, debug)) == p->hdr.sibling_ptr) {
        p = t;
        if(!p) {
            break;
        }
    }

    if(!t) {
        //printf("NOT FOUND %lu, t = %p\n", key, t);
        *f = false;
        return NULL;
    }

    *f = true;
    return (char *)t;
}

void btree::btree_scan_pred(entry_key_t key, int num, uint64_t buf[]){
    page* p = (page*)root;

    while(p->hdr.leftmost_ptr != NULL) {
        p = (page *)p->linear_search(key);
    }

    page *t;

    while((t = (page *)p->liner_scan(key, num, buf)) == p->hdr.sibling_ptr) {
        p = t;
        if(!p) {
            break;
        }
    }
    while (num > 0){
        buf[(--num)] = 0;
    }

}


char *btree::search(entry_key_t key) {
    bool f = false;
    char *ptr = btree_search_pred(key, &f, NULL);
    if (f) {
        return ptr;
    } else {
        ;//printf("not found.\n");
    }
    return NULL;
}

void btree::scan(entry_key_t key, int num, uint64_t buf[]){

    btree_scan_pred(key, num, buf);
}

// insert the key in the leaf node
void btree::btree_insert_pred(entry_key_t key, utree_value_t right, char **pred, bool *update){ //need to be string
    page* p = (page*)root;

    while(p->hdr.leftmost_ptr != NULL) {
        p = (page*)p->linear_search(key);
    }
    if(!p->store(this, NULL, key, right, true, true, pred)) { // store
        // The key already exist.
        *update = true;
    } else {
        // Insert a new key.
        *update = false;
    }
}

void btree::insert(entry_key_t key, char *right) {

    bool update;
    btree_insert_pred(key, (char *)right, NULL, &update);
}


void btree::remove(entry_key_t key) {
    btree_delete(key);
}

// store the key into the node at the given level
void btree::btree_insert_internal(char *left, entry_key_t key, char *right, uint32_t level) {
    if(level > ((page *)root)->hdr.level)
        return;

    page *p = (page *)this->root;

    while(p->hdr.level > level)
        p = (page *)p->linear_search(key);

    if(!p->store(this, NULL, key, right, true, true)) {
        btree_insert_internal(left, key, right, level);
    }
}

void btree::btree_delete(entry_key_t key) {
    page* p = (page*)root;

    while(p->hdr.leftmost_ptr != NULL){
        p = (page*) p->linear_search(key);
    }

    page *t;
    while((t = (page *)p->linear_search(key)) == p->hdr.sibling_ptr) {
        p = t;
        if(!p)
            break;
    }

    if(p) {
        if(!p->remove(this, key)) {
            btree_delete(key);
        }
    }
    else {
        printf("not found the key to delete %lu\n", key);
    }
}

void btree::printAll(){
    pthread_mutex_lock(&print_mtx);
    int total_keys = 0;
    page *leftmost = (page *)root;
    printf("root: %p\n", root);
    do {
        page *sibling = leftmost;
        while(sibling) {
            if(sibling->hdr.level == 0) {
                total_keys += sibling->hdr.last_index + 1;
            }
            sibling->print();
            sibling = sibling->hdr.sibling_ptr;
        }
        printf("-----------------------------------------\n");
        leftmost = leftmost->hdr.leftmost_ptr;
    } while(leftmost);

    printf("total number of keys: %d\n", total_keys);
    pthread_mutex_unlock(&print_mtx);
}
btree* init_utree(){
    btree* tree = new btree();
#ifdef USE_HMM
    tree->hybridMemory     = new HybridMemory(MaxThreadNum, 0.95, true, NULL, NULL, NULL);
#endif
    return tree;
}


int utree_insert(void *index, uint64_t key,
                  value_t val){
    btree * bTree = (btree*) index;
#ifdef USE_HMM
    LogAddress address = AddressNil;
    set_value(bTree->hybridMemory, address, key, &val);
    bTree->insert(key, (utree_value_t)address);
#else
    char * v = static_cast<char *>(malloc(sizeof(value_t) + 1));
    memcpy(v, val.value, value_size);
    bTree->insert(key, v);
#endif
    return 0;
}

value_t *utree_read(void *index, uint64_t key)
{
    btree * bTree = (btree*) index;

#ifdef USE_HMM
    LogAddress address=(LogAddress)bTree->search(key);
    value_t* v =  get_value_for_read(bTree->hybridMemory, address);
    return v;

#else
    char * ret = bTree->search(key);
    return reinterpret_cast<value_t *>(ret);
#endif
}

void utree_scan(void *index, uint64_t key, int range){
    btree * bTree = (btree*) index;
    uint64_t res[range + 1];
    value_t v;

#ifdef USE_HMM
    bTree->scan(key, range, res);
    for (int i = 0; i < range; ++i) {
        value_t* value_p = get_value_for_read(bTree->hybridMemory, res[i]);
        if(value_p != NULL){
            memcpy(v.value, (void *)value_p->value, value_size);
        }else{
            vmlog(WARN,"utree_scan:%ld, i:%d, range:%d\n", key, i, range);
            break;
        }
    }
#else
    bTree->scan(key, range, res);
    for (int i = 0; i < range; ++i) {
        if(res[i] == 0){
            vmlog(WARN, "utree_scan:%ld, i:%d, range:%d\n", key, i, range);
            break;
        } else{
            memcpy(v.value, (char*)res[i], value_size);
        }
    }
#endif
}