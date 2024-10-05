#include <iostream>
#include <chrono>
#include <random>
#include <cstring>
#include <vector>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <atomic>
#include <thread>
#include <sched.h>
#include <string.h>
#include <time.h>
#include <libconfig.h>
#include "hm.hpp"
#include "index/clht_lb_res.h"
#include "index/lf_chain_hashmap.h"
#include "index/bptree.h"
#include "index/bltree.h"
#include "index/bwtree.h"
#include "index/utree.h"

using namespace std;

static uint64_t LOAD_SIZE = 32000000;
static uint64_t RUN_SIZE = 32000000;
int threadNum = 1;
const char* init_file, *txn_file;


enum Operation{
    INSERT,
    READ,
    UPDATE,
    SCAN
};


//#define LATENCY
//#define W_LATENCY


typedef struct {
    unsigned long long  *values;
    unsigned long long  *tmp;
    unsigned long long  total;
    unsigned int        size;
    double              range_min;
    double              range_max;
    double              range_deduct;
    double              range_mult;
    pthread_mutex_t     mutex;
} sb_percentile_t;


sb_percentile_t percentile;
void percentile_update(double value)
{
    unsigned int n;

    if (value < percentile.range_min)
        value= percentile.range_min;
    else if (value > percentile.range_max)
        value= percentile.range_max;

    n = floor((log(value) - percentile.range_deduct) * percentile.range_mult
              + 0.5);

    pthread_mutex_lock(&percentile.mutex);
    percentile.total++;
    percentile.values[n]++;
    pthread_mutex_unlock(&percentile.mutex);
}

static int percentile_init(uint64_t size, double range_min, double range_max){
    percentile.values = (unsigned long long *)
            calloc(size, sizeof(unsigned long long));
    percentile.tmp = (unsigned long long *)
            calloc(size, sizeof(unsigned long long));
    if (percentile.values == NULL || percentile.tmp == NULL)
    {
        //log_text(LOG_FATAL, "Cannot allocate values array, size = %u", size);
        return 1;
    }

    percentile.range_deduct = log(range_min);
    percentile.range_mult = (size - 1) / (log(range_max) -
                                          percentile.range_deduct);
    percentile.range_min = range_min;
    percentile.range_max = range_max;
    percentile.size = size;
    percentile.total = 0;

    pthread_mutex_init(&percentile.mutex, NULL);

    return 0;
}


static double percentile_calculate(double percent)
{
    unsigned long long ncur, nmax;
    unsigned int       i;

    pthread_mutex_lock(&percentile.mutex);

    if (percentile.total == 0)
    {
        pthread_mutex_unlock(&percentile.mutex);
        return 0.0;
    }

    memcpy(percentile.tmp, percentile.values,
           percentile.size * sizeof(unsigned long long));
    nmax = floor(percentile.total * percent / 100 + 0.5);

    pthread_mutex_unlock(&percentile.mutex);

    ncur = percentile.tmp[0];
    for (i = 1; i < percentile.size; i++)
    {
        ncur += percentile.tmp[i];
        if (ncur >= nmax)
            break;
    }

    return exp((i) / percentile.range_mult + percentile.range_deduct);
}

uint64_t* init_keys;
uint64_t* txn_keys;
Operation* operations;
int* ranges;
uint64_t perTask = 10000;
std::atomic_int64_t taskNum;
uint64_t totalTNum = 0;


clht_t*  clht;
chain_hashmap_t chainHashMap;

b_root_obj* bptree;
bl_root_obj* bltree;
BwTree<uint64_t, bwtree_value_t> * bwtree;
btree* utree;
//#define test_bltree
//#define test_bwtree
//#define  test_utree
#ifdef test_clht
#define index clht
#define put_fn(key, value) clht_put(clht, key, value)
#define read_fn(key) (value_t*)clht_get(clht, key)
#define insertFn clht_put
#define recoveryInsertFn clht_put_recovery
#define scan_fn(key, r)

#endif
#ifdef test_lfht
#define index (&chainHashMap)
#define put_fn(key, value) lf_chain_put(&chainHashMap, key, value)
#define read_fn(key) (value_t*)lf_chain_find(&chainHashMap,  key)
#define insertFn NULL
#define recoveryInsertFn NULL
#define scan_fn(key, r)

#endif
#ifdef test_bptree
#define index (bptree)
#define put_fn(key, value) bp_tree_insert(bptree, key, value)
#define read_fn(key) (value_t*)bp_tree_read(bptree,  key)
#define insertFn NULL
#define recoveryInsertFn NULL
#endif
#ifdef test_bltree
#define index (bltree)
#define put_fn(key, value) bl_tree_insert(bltree, key, value)
#define read_fn(key) (value_t*)bl_tree_read(bltree,  key)
#define insertFn NULL
#define recoveryInsertFn NULL
#endif
#ifdef test_bwtree
#define index (bwtree)
#define put_fn(key, value) bwtree_insert(bwtree, key, value)
#define read_fn(key) (value_t*)bwtree_read(bwtree,  key)
#define insertFn NULL
#define recoveryInsertFn NULL
#endif
#ifdef test_utree
#define index (utree)
#define put_fn(key, value) utree_insert(utree, key, value)
#define read_fn(key) (value_t*)utree_read(utree,  key)
#define scan_fn(key, r) utree_scan(utree,  key, r)

#define insertFn NULL
#define recoveryInsertFn NULL
#endif
typedef struct ThreadAttributes{
    uint64_t whileCount;
    uint64_t waste;
    uint64_t newAddress;
    uint64_t invalidWhileCount;
    uint64_t txnCount;
    uint64_t nullCount;
    int threadId;
    double usedTime;
}ThreadAttributes;

#define traceLen 100000000
void loadData(ThreadAttributes* attributes){

    threadId = attributes->threadId;
#ifdef test_bwtree
    bwtree->AssignGCID(threadId);
#endif
//    vmlog(WARN, "load thread%", attributes->threadId);

    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    uint64_t tNum = taskNum.fetch_add(1);
    while ((tNum + 1) * perTask <= LOAD_SIZE){
        for (uint64_t i = 0; i < perTask; ++i) {
            //        value_t value = keys[i] + attributes->addValue;
            value_t value;
            put_fn(init_keys[i + tNum*perTask], value);
        }
        tNum = taskNum.fetch_add(1);

        if(tNum % 100 == 0){
            vmlog(WARN, "setValue :%ld", tNum);
        }
    }

    clock_gettime(CLOCK_REALTIME, &endTmp);
//    vmlog(WARN, "setValue end \n");

#ifdef USE_HMM
    index->hybridMemory->clientEnd();
#endif
#ifdef test_bwtree
    bwtree->UnregisterThread(threadId);
#endif
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
}
volatile bool endWork = false;
extern int total_search[64] ;
extern int travel_link[64];
void transaction(ThreadAttributes* attributes){
    threadId = attributes->threadId;
#ifdef test_bwtree
    bwtree->AssignGCID(threadId);
#endif
    vmlog(WARN, "thread%d", attributes->threadId);
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    uint64_t tNum = taskNum.fetch_add(1);
    char value_res[value_size];
    attributes->txnCount = 0;
    attributes->nullCount = 0;
    while (!endWork){
        tNum %= totalTNum;
        attributes->txnCount += perTask;
        for (uint64_t i = 0; i < perTask; ++i) {
            //        value_t value = keys[i] + attributes->addValue;
            switch (operations[i + tNum*perTask]) {
                case INSERT:
                    case UPDATE:
                    {
#ifdef W_LATENCY
                        struct timespec time1, time2;
                        clock_gettime(CLOCK_REALTIME, &time1);
#endif
                        value_t value;
                        put_fn(txn_keys[i + tNum*perTask], value);
#ifdef W_LATENCY
                        clock_gettime(CLOCK_REALTIME, &time2);
                        uint64_t latency = (time2.tv_sec- time1.tv_sec) * (uint64_t)1e9 + time2.tv_nsec - time1.tv_nsec;
                        percentile_update((double )latency);
#endif
                        break;
                    }
                    case READ:
                    {
#ifdef LATENCY
                        struct timespec time1, time2;
                        clock_gettime(CLOCK_REALTIME, &time1);
#endif
                        value_t* value_p = read_fn(txn_keys[i + tNum*perTask]);
                        if(value_p == NULL){
                            attributes->nullCount ++;
                        } else{
                            memcpy((void *)value_res, (void *)value_p->value, value_size);
                        }
#ifdef LATENCY
                        clock_gettime(CLOCK_REALTIME, &time2);
                        uint64_t latency = (time2.tv_sec- time1.tv_sec) * (uint64_t)1e9 + time2.tv_nsec - time1.tv_nsec;
                        percentile_update((double )latency);
#endif
                        break;
                    }
                case SCAN:
                {
                    scan_fn(txn_keys[i + tNum*perTask], ranges[i + tNum*perTask]);
                    break;
                }
                default:
                    vmlog(ERROR, "not support operation:%d, tNum:%d", operations[i + tNum*perTask], tNum);
            }

//            if(i % traceLen == 0){
//                vmlog(LOG, "setValue (%ld)",  + tNum*perTask);
//            }
        }

        tNum = taskNum.fetch_add(1);
        if(tNum % 500 == 0){
            vmlog(WARN, "transaction :%ld", tNum);
        }
    }

    clock_gettime(CLOCK_REALTIME, &endTmp);
    vmlog(LOG, "transaction end");
#ifdef USE_HMM
    index->hybridMemory->clientEnd();
#endif
#ifdef test_bwtree
    bwtree->UnregisterThread(threadId);
#endif
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
}
int64_t txnTime = 60; // s
int64_t warmTime = 30; // s

extern bool check_link;
extern uint64_t dram_hit[MaxThreadNum];
extern double recoveryTime;
void testClht(){
#ifdef NVM_ONLY
    init_nvmm_pool();
#endif
    auto starttime = std::chrono::system_clock::now();
    percentile_init(1e9, 1.0, 1e9);

    double initT = 0, txnT = 0;
    uint64_t  total_txn_count = 0, total_search_local = 0, total_travel_link = 0;
    pthread_t threadId[MaxThreadNum];
    ThreadAttributes attributes[MaxThreadNum];
    bool needLoad = true;
#ifdef test_clht
    int clht_para=27;
    int vs = value_size;
    clht = clht_create(1<<clht_para);
#endif
#ifdef test_lfht
    lf_chain_init(&chainHashMap);
#endif
#ifdef test_bptree
    bptree = init_bp_tree();
#endif
#ifdef test_bltree
    bltree = init_bl_tree();
#endif
#ifdef test_bwtree
    bwtree = init_bwtree();
    bwtree->UpdateThreadLocal(threadNum);
#endif
#ifdef test_utree
    utree = init_utree();
    vmlog(WARN, "after init_utree");
#endif
#ifdef DO_OpLog
    #ifndef MEM_STAT
    if(hm_recovery(index->hybridMemory, insertFn, recoveryInsertFn, index)){
        needLoad = false;
    }
    #endif
#endif

    vmlog(WARN, "berfore load need load:%d", needLoad);
    if(needLoad){
#ifdef USE_HMM

        index->hybridMemory->initClient(threadNum);
#endif

        taskNum.store(0);
        for (int i = 0; i < threadNum; ++i) {
            attributes[i]={
                    .threadId = i
            };
            pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(loadData), (void *) (attributes + i));
        }
        for (int i = 0; i < threadNum; ++i) {
            pthread_join(threadId[i], NULL);
            initT+= attributes[i].usedTime;
        }
    }
    vmlog(WARN, "after load", needLoad);

//    { //warm
//#ifdef USE_HMM
//        index->hybridMemory->initClient(threadNum);
//#endif
//        taskNum.store(0);
//        for (int i = 0; i < threadNum; ++i) {
//            attributes[i]={
//                    .threadId = i
//            };
//            pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(transaction), (void *) (attributes + i));
//        }
//        sleep(warmTime);
//        endWork = true;
//        for (int i = 0; i < MaxThreadNum; ++i) {
//            dram_hit[i] = 0;
//        }
//    }
#ifdef USE_HMM
    index->hybridMemory->initClient(threadNum);
#endif
#ifdef test_bwtree
    bwtree->UpdateThreadLocal(threadNum);
//    printf("before txn: epoch:%p, tree:%p\n", bwtree->epoch_manager.tree_p, bwtree);
#endif
    taskNum.store(0);
    check_link = true;
    endWork = false;
    vmlog(WARN, "berfore txn");
#ifndef MEM_STAT
    for (int i = 0; i < threadNum; ++i) {
        attributes[i]={
                .threadId = i
        };
        pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(transaction), (void *) (attributes + i));
    }
    sleep(txnTime);
    endWork = true;
    total_txn_count = 0;
    uint64_t total_null_count = 0;
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        txnT += attributes[i].usedTime;
        total_txn_count += attributes[i].txnCount;
        total_search_local += total_search[attributes[i].threadId];
        total_travel_link += travel_link[attributes[i].threadId];
        total_null_count += attributes[i].nullCount;
    }
    if(total_null_count){
        vmlog(WARN, "null count:%ld", total_null_count);
    }
#endif
    int  flag = 0;
    int default_para;
    int index_type = 0;
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
#ifdef DO_OpLog
    flag |= 128;
#endif
#ifdef test_clht
    index_type =1;
    default_para  = clht_para;
#endif
#ifdef test_lfht
    index_type =2;
    default_para = LMN_DEFAULT_SIZE;
#endif

#ifdef test_bptree
    flag |= 1024;
#endif
#ifdef test_bltree
    index_type =3;
    default_para = DEFAULT_TREE_ORDER;
#endif
#ifdef test_bwtree
    index_type =4;
#endif
#ifdef test_utree
    index_type = 5;
#endif
#ifdef NVM_ONLY
    flag |= 2048;
#endif
    uint64_t hit = 0;
    for (int i = 0; i < threadNum; ++i) {
        hit+= dram_hit[i];
    }
#ifdef MEM_STAT
    printf("%d, %d, %ld,  %ld, %d, %d,  %ld, "
           "%ld, %d, %ld, %.2f, "
           "%.2fMops, %.2fMops, %s, %ld\n",
           index_type, flag, LOAD_SIZE, RUN_SIZE, threadNum, value_size, txnTime,
           total_txn_count, default_para,
           DRAM_TOTAL_SIZE>>20, ((double )hit) / (total_txn_count * 1.0),
           LOAD_SIZE / perTask * perTask * threadNum / initT / 1e6,
           total_txn_count / txnTime / 1e6,
           init_file, totalPage.load() / 1024/1024);
#else
    double percentile_val50 = percentile_calculate(50);
    double percentile_val75 = percentile_calculate(75);
    double percentile_val99 = percentile_calculate(99);
    printf("%d, %d, %ld,  %ld, %d, %d,  %ld, "
           "%ld, %d, %ld, %.2f, "
           "%.2fMops, %.2fMops, %s, %.2fms, %.2f, %.2f,%.2f\n",
           index_type, flag, LOAD_SIZE, RUN_SIZE, threadNum, value_size, txnTime,
           total_txn_count, default_para,
           DRAM_TOTAL_SIZE>>20, ((double )hit) / (total_txn_count * 1.0),
           LOAD_SIZE / perTask * perTask * threadNum / initT / 1e6,
           total_txn_count / txnTime / 1e6,
           init_file, recoveryTime, percentile_val50,percentile_val75,percentile_val99);
#endif
    fflush(stdout);

}


void load_keys(const char* init_file){
    std::ifstream infile_load(init_file);

    std::string op;
    uint64_t key;
//    int range;

    std::string insert("INSERT");
    std::string read("READ");
    std::string scan("SCAN");
    std::string update("UPDATE");

    uint64_t count = 0;
    while ((count < LOAD_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return ;
        }
        init_keys[count] = key;
        count++;
        if(count % 10000000 == 0){
            vmlog(WARN, "Loaded %ld keys", count);
        }
    }

    vmlog(WARN, "Loaded %ld keys", count);
}

void load_txns(const char* txn_file){

    std::string op;
    uint64_t key;
//    int range;

    std::string insert("INSERT");
    std::string read("READ");
    std::string scan("SCAN");
    std::string update("UPDATE");

    uint64_t count = 0;

    std::ifstream infile_txn(txn_file);

    count = 0;
    int r = 0;
    while ((count < RUN_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;
        txn_keys[count] = key;
        if (op.compare(insert) == 0) {
            operations[count] = INSERT;
        } else if (op.compare(read) == 0) {
            operations[count] = READ;
        } else if (op.compare(scan) == 0) {
            operations[count] = SCAN;
            infile_txn >> r;
            ranges[count] = r;
        } else if (op.compare(update) == 0){
            operations[count] = UPDATE;
        } else{
            std::cout << "UNRECOGNIZED CMD!\n";
            return;
        }
        count++;
        if(count % 10000000 == 0){
            vmlog(WARN, "Loaded %ld txns", count);
        }
    }
    vmlog(WARN, "Loaded %ld txns\n", count);
}

void ycsb_load_run_randint(const char* init_file, const char* txn_file)
{
    pthread_t load_key_p, load_txn_p;
    pthread_create(&load_key_p, NULL, reinterpret_cast<void *(*)(void *)>(load_keys), (void *) (init_file));
    pthread_create(&load_txn_p, NULL, reinterpret_cast<void *(*)(void *)>(load_txns), (void *) (txn_file));

    pthread_join(load_key_p, NULL);
    pthread_join(load_txn_p, NULL);
}

int main(int argc, char **argv) {
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
    vmlog(WARN, "DRAM_TOTAL_SIZE:%ld, dram_size:%d", DRAM_TOTAL_SIZE, dram_size);
    config_lookup_int(&cfg, "threadNum", &threadNum);
    config_lookup_int64(&cfg, "LOAD_SIZE", reinterpret_cast<long long int *>(&LOAD_SIZE));
    config_lookup_int64(&cfg, "TXN_SIZE", reinterpret_cast<long long int *>(&RUN_SIZE));
    config_lookup_string(&cfg, "LOAD_FILE", &init_file);
    config_lookup_string(&cfg, "TXN_FILE", &txn_file);

    init_keys = (uint64_t*)malloc(sizeof(uint64_t) * LOAD_SIZE);
    txn_keys = (uint64_t*)malloc(sizeof(uint64_t) * RUN_SIZE);
    operations = (Operation*) malloc(sizeof(Operation) * RUN_SIZE);
    ranges = (int*) malloc(sizeof(int ) * RUN_SIZE);
    memset(ranges, 0, sizeof(int ) * RUN_SIZE);

    ycsb_load_run_randint(init_file, txn_file);
    totalTNum = RUN_SIZE / perTask;
    testClht();
    return 0;
}
