
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree.cpp
//
// Identification: src/index/bwtree.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "bwtree.h"


bool print_flag = false;

// This will be initialized when thread is initialized and in a per-thread
// basis, i.e. each thread will get the same initialization image and then
// is free to change them
thread_local int BwTreeBase::gc_id = -1;

std::atomic<size_t> BwTreeBase::total_thread_num{0UL};


BwTree<uint64_t, bwtree_value_t> * init_bwtree(){
    auto * bwTree = new BwTree<uint64_t, bwtree_value_t>();
#ifdef USE_HMM
    bwTree->hybridMemory     = new HybridMemory(MaxThreadNum, 0.8, true, NULL, NULL, NULL);
#endif
    return bwTree;
}

int bwtree_insert(void *index, uint64_t key,
                   value_t val)
{
    auto* bwTree = (BwTree<uint64_t, bwtree_value_t>*) index;
#ifdef USE_HMM
    LogAddress address = AddressNil;
    set_value(bwTree->hybridMemory, address, key, &val);
    bwTree->Insert(key, (uint64_t)address);
#else
    bwTree->Insert(key, val);
#endif
    return 0;
}

value_t *bwtree_read(void *index, uint64_t key)
{
    auto* bwTree = (BwTree<uint64_t, bwtree_value_t>*) index;
    value_t* val = nullptr;

#ifdef USE_HMM
    std::vector<bwtree_value_t> value_vector{};
    value_vector.reserve(1);

    bwTree->GetValue(key, value_vector);
    if(!value_vector.empty()){
        LogAddress address= value_vector[0];
        val = get_value_for_read(bwTree->hybridMemory, address);
    }
#else
    std::vector<bwtree_value_t> value_vector{};
    value_vector.reserve(1);

    bwTree->GetValue(key, value_vector);
    if(!value_vector.empty()){
        val = &value_vector[0];
    }
#endif

    return val;
/*	if (!val)
		printf("key %ld not found\n", key);
	else
		printf("Record at %p -- key %ld, val %s\n",
				val, key, val);*/
}