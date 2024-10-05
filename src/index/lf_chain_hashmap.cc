/**
 * @file   lf_chain_hashmap.cc
 * @brief  
 * @author Taketo Yoshida
 */
#include "lf_chain_hashmap.h"
#include "thread.h"

/*
 * public functions
 */

void lf_chain_init(chain_hashmap_t* map) {
#ifdef NVM_ONLY
    map->tbl              = (chain_entry_t**) nvm_alloc(sizeof(chain_entry_t*) * LMN_DEFAULT_SIZE);
#else
  map->tbl              = (chain_entry_t**)calloc(sizeof(chain_entry_t*), LMN_DEFAULT_SIZE);
#endif
#ifdef MEM_STAT
    totalPage.fetch_add(LMN_DEFAULT_SIZE*sizeof(chain_entry_t*));
#endif
  map->bucket_mask      = LMN_DEFAULT_SIZE- 1;
  map->size             = 0;
#ifdef USE_HMM
  map->hybridMemory     = new HybridMemory(MaxThreadNum, 0.95, true, NULL, NULL, NULL);
#endif
  memset(map->tbl, 0x00, LMN_DEFAULT_SIZE);
}

value_t* lf_chain_find(chain_hashmap_t *map, lmn_key_t key) {
    lmn_word bucket     = lmn_hash(key) & map->bucket_mask;
  chain_entry_t *ent  = map->tbl[bucket];
//  chain_entry_t *old;

  while(ent != LMN_HASH_EMPTY) {
    if (ent->key == key) {
#ifdef USE_HMM
        return get_value_for_read(map->hybridMemory, ent->data);
#else
      return (lmn_data_t*)&ent->data;
#endif
    }
    ent = ent->next;
  }
  return NULL;
}

void lf_chain_free(chain_hashmap_t* map) {
}

#ifdef USE_HMM
void lf_chain_put(chain_hashmap_t *map, lmn_key_t key, value_t data) {
#else
void lf_chain_put(chain_hashmap_t *map, lmn_key_t key, lmn_data_t data) {
#endif
    lmn_word bucket        = lmn_hash(key) & map->bucket_mask;
  chain_entry_t **ent    = &map->tbl[bucket];

  if ((*ent) == LMN_HASH_EMPTY) {
    chain_entry_t   *new_ent = NULL;
     if (new_ent == NULL) {
#ifdef NVM_ONLY
         new_ent = (chain_entry_t*) nvm_alloc(sizeof(chain_entry_t));
#else
      new_ent = (chain_entry_t*)malloc(sizeof(chain_entry_t));
#endif
#ifdef MEM_STAT
         totalPage.fetch_add(sizeof(chain_entry_t));
#endif
      new_ent->next = NULL;
      if(!LMN_CAS(&(*ent), NULL, new_ent)) {
        if (new_ent == NULL) free(new_ent);
        return lf_chain_put(map, key, data);
      }
    } else {
      return lf_chain_put(map, key, data);
    }
  } else {
    chain_entry_t *cur, *tmp, *new_ent = NULL;
    cur = *ent;
    do {
      tmp = *ent;
      do {
        if (cur->key == key) {
#ifdef USE_HMM
            set_value(map->hybridMemory, cur->data, key, &data);
#else
            cur->data = data;
#endif

          if (new_ent == NULL) free(new_ent);
          return;
        }
        //LMN_DBG("[debug] cc_hashmap_put_inner: retry hash, key %u, thread:%d\n", key, GetCurrentThreadId());
        //printf("%p ", cur->next);
      } while(cur->next != LMN_HASH_EMPTY && (cur = cur->next));
      if (new_ent == NULL)
#ifdef NVM_ONLY
            new_ent = (chain_entry_t*) nvm_alloc(sizeof(chain_entry_t));
#else
        new_ent = (chain_entry_t*)malloc(sizeof(chain_entry_t));
#endif
#ifdef MEM_STAT
        totalPage.fetch_add(sizeof(chain_entry_t));
#endif
      new_ent->next = (*ent);
    } while(!LMN_CAS(&(*ent), tmp, new_ent));
    // dbgprint("insert key:%d, data:%d\n", key, data);
  }
  (*ent)->key  = key;
#ifdef USE_HMM
  (*ent)->data=AddressNil;
  set_value(map->hybridMemory, (*ent)->data, key, &data);
#else
  (*ent)->data = data;
#endif

  //LMN_ATOMIC_ADD(&(map->size), 1);
}


