//
// Created by workshop on 4/8/2022.
//

#include <libconfig.h>
#include "hm.hpp"
#include "index/clht_lb_res.h"
#include "index/chain_hashmap.h"
#include "index/lf_chain_hashmap.h"
#include "index/bptree.h"


clht_t*  clht;
chain_hashmap_t chainHashMap;
uint64_t* keys;
extern thread_local int threadId;
int threadNum = 1;
uint64_t total = 10000000;
uint64_t perTask = 10000;
std::atomic_int64_t taskNum;
b_root_obj* bptree;

#define test_clht


#ifdef test_clht
#define index clht
#define put(key, value) clht_put(clht, key, value)
#define read(key) (value_t*)clht_get(clht, key)
#else
#ifdef test_lfht
#define index (&chainHashMap)
#define put(key, value) lf_chain_put(&chainHashMap, key, value)
#define read(key) (value_t*)lf_chain_find(&chainHashMap,  key)
#endif
#endif
#ifdef test_bptree
#define index (bptree)
#define put(key, value) bp_tree_insert(bptree, key, value)
#define read(key) (value_t*)bp_tree_read(bptree,  key)
#endif

typedef struct IndexAttributes{
    uint64_t addValue;
    uint64_t whileCount;
    uint64_t waste;
    uint64_t newAddress;
    uint64_t invalidWhileCount;
    int threadId;
    double usedTime;
}IndexAttributes;
#define traceLen 10000000

void setValue(IndexAttributes* attributes){
    threadId = attributes->threadId;
    //    printf("thread%d: (%d, %d)\n ", attributes->threadId,  attributes->startPos, attributes->endPos);
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    uint64_t tNum = taskNum.fetch_add(1);
    vmlog(LOG, "thread:%d, taskNum:%ld, perTask:%ld", threadId, tNum, perTask);
    while ((tNum + 1) * perTask <= total){
        for (uint64_t i = 0; i < perTask; ++i) {
            //        value_t value = keys[i] + attributes->addValue;
            value_t value;
            put(keys[i + tNum*perTask], value);
#ifdef __DEBUG
            if(i % traceLen == 0){
                vmlog(LOG, "setValue (%ld), writtenPages:%ld, allocatedPages:%ld", i, index->hybridMemory->dramWriteRegionMeta->writtenPages.load(),
                      index->hybridMemory->dramWriteRegionMeta->allocatedPages);
            }
#endif
        }
        tNum = taskNum.fetch_add(1);
        vmlog(LOG, "thread:%d, taskNum:%d", threadId, tNum);
    }


    clock_gettime(CLOCK_REALTIME, &endTmp);
    vmlog(LOG, "setValue end");
#ifdef USE_HMM
    index->hybridMemory->clientEnd();
#endif
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
}
void readValue(IndexAttributes* attributes){
    threadId = attributes->threadId;
    //    printf("thread%d: (%d, %d)\n ", attributes->threadId,  attributes->startPos, attributes->endPos);
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    char value[value_size];
    uint64_t tNum = taskNum.fetch_add(1);
    vmlog(LOG, "thread:%d, taskNum:%ld", threadId, tNum);
    while ((tNum + 1) * perTask <= total){
        for (uint64_t i = 0; i < perTask; ++i) {
            value_t* value_p = read(keys[i + tNum*perTask]);
            if(value_p == NULL){
                vmlog(WARN, "e(%d, NULL)", keys[i + tNum*perTask]);
            } else{
                memcpy(value, (void *)value_p->value, value_size);
            }
#ifdef CHECHRes
            if(value_p == NULL){
                vmlog(WARN, "e(%d, NULL)", keys[i + tNum*perTask]);
            } else if(*value_p != keys[i + tNum*perTask] + attributes->addValue){
                vmlog(WARN, "e(%ld, %ld, %ld)", keys[i + taskNum*perTask], value, attributes->addValue + keys[i]);
            }
#endif
        }
        tNum = taskNum.fetch_add(1);
        vmlog(LOG, "thread:%d, taskNum:%d", threadId, tNum);
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
#ifdef USE_HMM
    index->hybridMemory->clientEnd();
#endif
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    vmlog(LOG, "getValue end");
}
void testClht(){
    double putT = 0, updateT = 0, readT1 = 0, readT2 = 0, readT3 = 0;
    keys = (uint64_t*)malloc(sizeof(uint64_t) * total);
#ifdef test_clht
    clht = clht_create(1<<25);
#endif
#ifdef test_lfht
    lf_chain_init(&chainHashMap);
#endif
    for (uint64_t i = 0; i < total; ++i) {
        keys[i] = i + 1;
    }
    pthread_t threadId[MaxThreadNum];
    IndexAttributes attributes[MaxThreadNum];
#ifdef USE_HMM
    index->hybridMemory->initClient(threadNum);
#endif
    taskNum.store(0);
    for (int i = 0; i < threadNum; ++i) {
        attributes[i]={
                .addValue = 0,
                .threadId = i
        };
        pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(setValue), (void *) (attributes + i));
    }
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        putT += attributes[i].usedTime;
    }

#ifdef USE_HMM
    index->hybridMemory->initClient(threadNum);
#endif
    taskNum.store(0);
    for (int i = 0; i < threadNum; ++i) {
        pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(readValue), (void *) (attributes + i));
    }
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        readT1 += attributes[i].usedTime;
    }

#ifdef USE_HMM
    index->hybridMemory->initClient(threadNum);
#endif
    taskNum.store(0);
    for (int i = 0; i < threadNum; ++i) {
        pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(readValue), (void *) (attributes + i));
    }
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        readT3 += attributes[i].usedTime;
    }

#ifdef USE_HMM
    index->hybridMemory->initClient(threadNum);
#endif
    taskNum.store(0);
    for (int i = 0; i < threadNum; ++i) {
        attributes[i]={
                .addValue = total,
                .threadId = i
        };
        pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(setValue), (void *) (attributes + i));
    }
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        updateT += attributes[i].usedTime;
    }

#ifdef USE_HMM
    index->hybridMemory->initClient(threadNum);
#endif
    taskNum.store(0);
    for (int i = 0; i < threadNum; ++i) {
        pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(readValue), (void *) (attributes + i));
    }
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        readT2 += attributes[i].usedTime;
    }

    int  flag = 0;
#ifdef USE_NVM
    flag |= 1;
#endif
#ifdef COMPRESS
    flag |= 2;
#endif
#ifdef INVALID
    flag |= 4;
#endif
#ifdef INVALID_PERSIST
    flag |= 8;
#endif
#ifdef USE_Cache
    flag |= 16;
#endif
#ifdef MULTI_WRITE
    flag |= 32;
#endif
#ifdef USE_HMM
    flag |= 64;
#endif
#ifdef test_clht
    flag |= 128;
#endif
#ifdef test_lfht
    flag |= 256;
#endif
    printf("%X, %ld, %d, %d, %.2fMops, %.2fMops, %.2fMops, %.2fMops, %.2fMops\n",
           flag, total, threadNum, value_size,
           total / perTask * perTask * threadNum / putT / 1e6, total / perTask * perTask * threadNum/updateT / 1e6,
           total / perTask * perTask * threadNum / readT1 / 1e6, total / perTask * perTask * threadNum / readT3 / 1e6,
           total / perTask * perTask * threadNum/readT2 / 1e6);
    fflush(stdout);

}


int main(){
    const char ConfigFile[]= "config.cfg";

    config_t cfg;

    config_init(&cfg);
    cfg.root->type = CONFIG_TYPE_INT64;

    /* Read the file. If there is an error, report it and exit. */
    if(! config_read_file(&cfg, ConfigFile))
    {
        fprintf(stderr, "%s:%d - %s\n", config_error_file(&cfg),
                config_error_line(&cfg), config_error_text(&cfg));
        config_destroy(&cfg);
        return(EXIT_FAILURE);
    }
    double d;
    config_lookup_float(&cfg, "total", &d);
    config_lookup_int(&cfg, "threadNum", &threadNum);
    total = (uint64_t)d;
    testClht();
    return 1;
}