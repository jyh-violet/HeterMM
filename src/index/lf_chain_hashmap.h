/**
 * @file   lf_chain_hashmap.h
 * @brief  
 * @author Taketo Yoshida
 */
#ifndef LF_CHAIN_HASHMAP_H
#  define LF_CHAIN_HASHMAP_H

#include "chain_hashmap.h"

void lf_chain_init(chain_hashmap_t* map);
value_t* lf_chain_find(chain_hashmap_t *map, lmn_key_t key);
#ifdef USE_HMM
void lf_chain_put(chain_hashmap_t *map, lmn_key_t key, value_t data);
#else
    void lf_chain_put(chain_hashmap_t *map, lmn_key_t key, lmn_data_t data);
#endif
void lf_chain_free(chain_hashmap_t* map);

#endif /* ifndef LF_CHAIN_HASHMAP_H */

