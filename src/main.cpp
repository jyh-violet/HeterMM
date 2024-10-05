#include <iostream>
#include <libconfig.h>
#include <csignal>
#include <execinfo.h>
#include "index/hashmap.h"
#include "hm.hpp"

//#define IndexType lmntal_concurrent_hashmap::hashmap_t
//#define put(index, k, v) lmntal_concurrent_hashmap::hashmap_put(index, k, v)
//#define get(index, k) lmntal_concurrent_hashmap::hashmap_find(index, k)
//
//#define init(index)  lmntal_concurrent_hashmap::hashmap_init(&index, lmntal_concurrent_hashmap::LMN_CLOSED_ADDRESSING);

#define IndexType LogAddress*
#define put(index, k, v) (*index)[k] = v
#define get(index, k) ((*index) + k)

#define init(index)  index = (LogAddress*)malloc(sizeof (LogAddress) * total)
extern thread_local int threadId;
int threadNum = 1;
uint64_t total = 10000000;
extern thread_local uint64_t whileCount;
extern thread_local uint64_t waste;
extern thread_local uint64_t newAddress;
extern thread_local uint64_t invalidWhileCount;
typedef struct ThreadAttributes{
    HybridMemory* hm;
    IndexType* index;
    uint64_t startPos;
    uint64_t endPos;
    uint64_t addValue;
    int threadId;
    double usedTime;
    uint64_t whileCount;
    uint64_t waste;
    uint64_t newAddress;
    uint64_t invalidWhileCount;
}ThreadAttributes;
#define Retry 10
#define traceLen 100000000
void setValue(ThreadAttributes* attributes){
    threadId = attributes->threadId;
//    printf("thread%d: (%d, %d)\n ", attributes->threadId,  attributes->startPos, attributes->endPos);
    HybridMemory* hm = attributes->hm;
    IndexType* index = attributes->index;
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    for (uint64_t i = attributes->startPos; i < attributes->endPos; ++i) {
//        value_t value = i + attributes->addValue;
        value_t value;
        LogAddress* oldaddress = get(index, i);
        *oldaddress = (attributes->addValue > 0)? (*oldaddress) : AddressNil;
        set_value(hm, *oldaddress, i, &value);
        if(*oldaddress %8 !=0){
            vmlog(WARN, "wrong address:%ld", oldaddress);
        }
        if(i % traceLen == 0){
            vmlog(LOG, "setValue (%ld, %ld), globalDRAM:%d, allocatedPage:%d, "
                       "usedPage:%d, readOnly:%d, pagesBeforeReadOnly:%ld, writtenPages:%ld, safeForFree:%d",
                       i, *oldaddress, hm->globalDRAMPages, hm->dramWriteRegionMeta->allocatedPages,
                  hm->dramWriteRegionMeta->usedPages, hm->perThreadMeta[threadId].readOnlyPages,
                  hm->pagesBeforeReadOnly, hm->dramWriteRegionMeta->writtenPages.load(), hm->safeForFreePages);
        }
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    hm->clientEnd();
    vmlog(LOG, "setValue end, whileCount:%ld, waste:%ld, newAddress:%ld",
          whileCount, waste, newAddress);
    attributes->whileCount = whileCount;
    attributes->waste = waste;
    attributes->newAddress = newAddress;
    attributes->invalidWhileCount = invalidWhileCount;
    invalidWhileCount = 0;
    whileCount = 0;
    waste = 0;
    newAddress = 0;
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
}
void readValue(ThreadAttributes* attributes){
    threadId = attributes->threadId;
    //    printf("thread%d: (%d, %d)\n ", attributes->threadId,  attributes->startPos, attributes->endPos);
    HybridMemory* hm = attributes->hm;
    IndexType* index = attributes->index;
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    for (uint64_t i = attributes->startPos; i < attributes->endPos; ++i) {
        LogAddress*  address = get(index, i);
//        value_t value = i + attributes->addValue;
//        vmlog(LOG, "get (%d, %ld)", i, address);
        value_t *value_c;
        value_c = get_value_for_read(hm,  *address);
        value_t v = *value_c;
        #ifdef CHECHRes
//        if((value_c == NULL) || (value_c->value == NULL)){
//            vmlog(WARN, "e(%ld,%ld,%ld,%ld, %ld):globalDRAM:%d\n",
//                  i, *address, *value_c,value, (*(value_c) < 2 * total)?
//                        ((*(value_c) < total)? *get(index, (*(value_c))): *get(index, (*(value_c))-total))
//                        : 0,
//                  hm->globalDRAMPages);
//        }
        #endif
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    hm->clientEnd();
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    vmlog(LOG, "getValue end");
}

void testHybridMM(){
    HybridMemory hm(threadNum, 0.8, false, NULL, NULL, NULL);
    double putT = 0, updateT = 0, readT1 = 0, readT2 = 0, readT3 = 0;
    uint64_t totalWhileCount1 = 0, totalWhileCount2 = 0, totalWaste1 = 0, totalWaste2 = 0;
    uint64_t totalNewAddress1 = 0, totalNewAddress2 = 0;
    uint64_t totalInvalidWhile1 = 0, totalInvalidWhile2 = 0;
    IndexType index;
    init(index);

    if(index == NULL){
        vmlog(ERROR, "index init error");
    }

    for (uint64_t i = 0; i < total; ++i) {
        index[i] = AddressNil;
    }

    pthread_t threadId[MaxThreadNum];
    ThreadAttributes attributes[MaxThreadNum];
    uint64_t perThread = total / threadNum;
    hm.initClient(threadNum);
    for (int i = 0; i < threadNum; ++i) {
        attributes[i]={
                .hm = &hm,
                .index = &index,
                .startPos = perThread * i,
                .endPos = (i == (threadNum - 1))? (total): (perThread * (i + 1)),
                .addValue = 0,
                .threadId = i
        };
        pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(setValue), (void *) (attributes + i));
    }
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        putT += attributes[i].usedTime;
        totalWhileCount1 += attributes[i].whileCount;
        totalWaste1 += attributes[i].waste;
        totalNewAddress1 += attributes[i].newAddress;
        totalInvalidWhile1 += attributes[i].invalidWhileCount;
    }

    hm.initClient(threadNum);
    for (int i = 0; i < threadNum; ++i) {
        pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(readValue), (void *) (attributes + i));
    }
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        readT1 += attributes[i].usedTime;
    }

    hm.initClient(threadNum);
    for (int i = 0; i < threadNum; ++i) {
        pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(readValue), (void *) (attributes + i));
    }
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        readT3 += attributes[i].usedTime;
    }

    hm.initClient(threadNum);
    for (int i = 0; i < threadNum; ++i) {
        attributes[i]={
                &hm,
                &index,
                perThread * i,
                (i == (threadNum - 1))? (total): (perThread * (i + 1)),
                total,
                i
        };
        pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(setValue), (void *) (attributes + i));
    }
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        updateT += attributes[i].usedTime;
        totalWhileCount2 += attributes[i].whileCount;
        totalWaste2 += attributes[i].waste;
        totalNewAddress2 += attributes[i].newAddress;
        totalInvalidWhile2 += attributes[i].invalidWhileCount;
    }

    hm.initClient(threadNum);
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
//    printf("%.2fs, %.2fs, %.2fs, %.2fs\n",
//           putT, updateT, readT1 , readT2);
//    printf("%d, %ld, %d, %d,  %.2fMops, %.2fMops, %.2fMops, %.2fMops, %.2fMops, "
//           "whileCount:(%ld, %ld), waste:(%ld, %ld), newAddress:(%ld, %ld), invalidWhile:(%ld, %ld)"
//           "page:%d, block:%d, prepare:%d, after:%d\n",
//           flag, total, threadNum, value_size,
//           total * threadNum / putT / 1e6, total * threadNum/updateT / 1e6,
//           total * threadNum / readT1 / 1e6, total * threadNum / readT3 / 1e6, total * threadNum/readT2 / 1e6,
//           totalWhileCount1, totalWhileCount2, totalWaste1, totalWaste2, totalNewAddress1, totalNewAddress2,
//           totalInvalidWhile1, totalInvalidWhile2,
//           PageSizeBits, BlockSizeBits, preparePersisPages, pagesAfterReadOnly);
    printf("%d, %ld, %d, %d,  %.2fMops, %.2fMops, %.2fMops, %.2fMops, %.2fMops, "
           "%ld, %ld,  %ld, %ld,  %ld, %ld,  %ld, %ld,  "
           "%d, %d, %d, %d\n",
           flag, total, threadNum, value_size,
           total * threadNum / putT / 1e6, total * threadNum/updateT / 1e6,
           total * threadNum / readT1 / 1e6, total * threadNum / readT3 / 1e6, total * threadNum/readT2 / 1e6,
           totalWhileCount1, totalWhileCount2, totalWaste1, totalWaste2, totalNewAddress1, totalNewAddress2,
           totalInvalidWhile1, totalInvalidWhile2,
           PageSizeBits, BlockSizeBits, preparePersisPages, pagesAfterReadOnly);
    fflush(stdout);
}
void OnSIGSEGV(int signum, siginfo_t *info, void *ptr){

//TO DO: 输出堆栈信息
vmlog(WARN, "SIGSEGV!!!!!!!! fault si_code:%d", info->si_code);
    void * array[25]; /* 25 层，太够了 : )，你也可以自己设定个其他值 */

    int nSize = backtrace(array, sizeof(array)/sizeof(array[0]));
    char** str = backtrace_symbols(array, nSize);

    for (int i=0; i < nSize; i++){ /* 头尾几个地址不必输出，看官要是好奇，输出来看看就知道了 */

         /* 修正array使其指向正在执行的代码 */
        vmlog(WARN, " %s\n", str[i]);
    }
    abort();
}
int main() {
//    struct sigaction act;
//
//    int sig = SIGSEGV;
//
//    sigemptyset(&act.sa_mask);
//
//    act.sa_sigaction = OnSIGSEGV;
//
//    act.sa_flags = SA_SIGINFO;
//
//    if(sigaction(sig, &act, NULL)<0)
//
//    {
//
//        perror("sigaction:");
//
//    }

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

    testHybridMM();
    return 0;
}
