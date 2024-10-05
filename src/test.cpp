//
// Created by workshop on 3/9/2022.
//
#include <iostream>
#include "hm.hpp"
/*
void testNVM(){
    char filename[FileNameMaxLen];
    getNVMFileName(filename, 100000);
    if(access(filename, F_OK) == 0){
        remove(filename);
    }
    size_t mapped_len;
    int is_pmem = 1;
    size_t  totalLen = getInvalidBlockSize;
    char* dest = (char*) pmem_map_file(filename, totalLen,
                                       PMEM_FILE_CREATE|PMEM_FILE_EXCL,
                                       0666, &mapped_len, &is_pmem);
    char src[1<<20];
    int stepLen= 1 << PageSizeBits;
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    for (size_t i = 0; (i + stepLen) < totalLen; i+= stepLen) {
        memcpy(dest + i, src, stepLen);
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    double usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    printf("memcpy throughput:%.2fGB/s\n", (totalLen >> 30) / usedTime);
}
void testPlus(){
    uint64_t total = 1L<<32;
    uint64_t num = 0;
    struct timespec startTmp, endTmp;
    double usedTime ;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    for (size_t i = 0;i < total; i ++) {
        num ++;
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    printf("++ throughput:%.2lfGops\n", (total >> 30) / usedTime);

    num = 0;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    for (size_t i = 0;i < total; i ++) {
        __sync_fetch_and_add (&num, 1);
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    printf("sac throughput:%.2lfGops\n", (total >> 30) / usedTime);

//    atomic_uint64_t atomicUint64;
//    atomicUint64.store(0);
//    clock_gettime(CLOCK_REALTIME, &startTmp);
//
//    for (size_t i = 0;i < total; i ++) {
//        atomicUint64.fetch_add(1);
//    }
//    clock_gettime(CLOCK_REALTIME, &endTmp);
//    usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
//    printf("atomic_uint64_t throughput:%.2fGops\n", (total >> 30) / usedTime);


//    for (size_t i = 0;i < total; i ++) {
//        hm.dramOffset ++;
//    }
//    clock_gettime(CLOCK_REALTIME, &endTmp);
//    usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
//    printf("volatile ++ throughput:%.2fGops\n", (total >> 30) / usedTime);
//
//    num = 0;
//    clock_gettime(CLOCK_REALTIME, &startTmp);
//    for (size_t i = 0;i < total; i ++) {
//        if(hm.dramOffset > 0){
//            num ++;
//        }
//    }
//    clock_gettime(CLOCK_REALTIME, &endTmp);
//    usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
//    printf("volatile read throughput:%.2fGops\n", (total >> 30) / usedTime);
}

void testFlag(){
    size_t total = 1L<<32;
    uint64_t flag = 0;
    struct timespec startTmp, endTmp;
    double usedTime ;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    for (size_t i = 0;i < total; i ++) {
        flag |= 2;
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    printf("|= throughput:%.2fGops/s\n", (total >> 30) / usedTime);

    flag = 0;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    for (size_t i = 0;i < total; i ++) {
        __sync_fetch_and_or (&flag, 2);
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    printf("sac |= throughput:%.2fGops/s\n", (total >> 30) / usedTime);

    clock_gettime(CLOCK_REALTIME, &startTmp);
    for (size_t i = 0;i < total; i ++) {
        flag &= 2;
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    printf("&= throughput:%.2fGops/s\n", (total >> 30) / usedTime);

    flag = 0;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    for (size_t i = 0;i < total; i ++) {
        __sync_fetch_and_and (&flag, 2);
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    printf("sac &= throughput:%.2fGops/s\n", (total >> 30) / usedTime);



}

typedef struct WriteAttribute{
    char* src;
    char* dest;
    int pageNum;
    std::atomic_bool needWrite;
    bool workEnd;
}WriteAttribute;

void WriteThread(WriteAttribute* attribute){
    while (!attribute->workEnd){
        if(attribute->needWrite){
            for (int i = 0; i < attribute->pageNum; ++i) {
                char* dest = attribute->dest + i * getPageSize;
                char* src = attribute->src + i * getPageSize;
//                memcpy(dest, src, getPageSize);
                nt_store(dest, src, getPageSize);
            }
            attribute->needWrite.store(false);
        }
    }
}

void testSingleWrite(){
    char* dataSpace;
    posix_memalign((void**)(&dataSpace), Alignment, DRAM_TOTAL_SIZE);
    memset(dataSpace, 0, DRAM_TOTAL_SIZE);

    uint64_t blockNums = DRAM_TOTAL_SIZE / getBlockSize;
    char* nvm[blockNums];
    for (uint64_t i = 0; i < blockNums; ++i) {
        char filename[FileNameMaxLen];
        getNVMFileName(filename, (int )i + 10000);
        nvm[i] = mallocNVM(filename, getBlockSize);
    }

    struct timespec startTmp, endTmp;
    double usedTime ;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    for (uint64_t i = 0; i < blockNums; ++i) {
        char* block = dataSpace + i * getBlockSize;
        for (int j = 0; j < getBlockSize / getPageSize; ++j) {
            char* dest = nvm[i] + j * getPageSize;
            char* src = block + j * getPageSize;
//            memcpy(dest, src, getPageSize);
            nt_store(dest, src, getPageSize);
        }

    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    printf("single thread %.2fs, %.2fGps, page:%d, block:%d\n",
           usedTime, (DRAM_TOTAL_SIZE>>30) / usedTime, PageSizeBits, BlockSizeBits);
    free(dataSpace);
#ifdef USE_NVM
    for(uint64_t i = 0; i < blockNums; ++i){
        pmem_unmap(nvm[i], getBlockSize);
    }
#else
    for(uint64_t i = 0; i < blockNums; ++i){
        free(nvm[i]);
    }
#endif
}

void testMultiWrite(int persistThreadNum ){
    char* dataSpace;
    posix_memalign((void**)(&dataSpace), Alignment, DRAM_TOTAL_SIZE);
    memset(dataSpace, 0, DRAM_TOTAL_SIZE);

    uint64_t blockNums = DRAM_TOTAL_SIZE / getBlockSize;
    char* nvm[blockNums];
    for (uint64_t i = 0; i < blockNums; ++i) {
        char filename[FileNameMaxLen];
        getNVMFileName(filename, (int )i + 10000);
        nvm[i] = mallocNVM(filename, getBlockSize);
    }

    struct timespec startTmp, endTmp;
    double usedTime ;
    volatile WriteAttribute attributes[MaxThreadNum];
    pthread_t threads[persistThreadNum];
    for (int i = 0; i < persistThreadNum; ++i) {
        attributes[i].needWrite = false;
        attributes[i].workEnd = false;
        pthread_create(threads + i, NULL,
                       (void *(*)(void *))(WriteThread), (void *)(attributes + i));
    }

    clock_gettime(CLOCK_REALTIME, &startTmp);
    for (uint64_t i = 0; i < blockNums; ++i) {
        char* block = dataSpace + (i) * getBlockSize;
        char* dest = nvm[i] ;
        int perThreadPageNum = (getBlockSize / getPageSize + persistThreadNum - 1)/ persistThreadNum;
        for (int j = 0; j < persistThreadNum; ++j) {
            attributes[j].src = block + j * perThreadPageNum * getPageSize;
            attributes[j].pageNum = (j == (persistThreadNum - 1))?
                    (getBlockSize / getPageSize - j * perThreadPageNum) : perThreadPageNum;
            attributes[j].dest = (dest + j * perThreadPageNum * getPageSize);
            attributes[j].needWrite.store(true);
        }
        //wait for all thread end
        for (int j = 0; j < persistThreadNum; ++j) {
            while (attributes[j].needWrite.load()){
            }
        }
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    printf("%d threads %.2fs, %.2fGps, page:%d, block:%d\n",
           persistThreadNum, usedTime, (DRAM_TOTAL_SIZE>>30) / usedTime, PageSizeBits, BlockSizeBits);
    for (int i = 0; i < persistThreadNum; ++i) {
        attributes[i].workEnd = true;
        pthread_join(threads[i], NULL);

    }
    free(dataSpace);
#ifdef USE_NVM
    for(uint64_t i = 0; i < blockNums; ++i){
        pmem_unmap(nvm[i], getBlockSize);
    }
#else
    for(uint64_t i = 0; i < blockNums; ++i){
        free(nvm[i]);
    }
#endif
}

int main() {
//    testNVM();
//    testPlus();
//    testFlag();
    for (int j = 0; j < 10; ++j) {
        testSingleWrite();
    }
    printf("\n\n");
    for (int i = 2; i < 16; i++) {
        for (int j = 0; j < 10; ++j) {
            testMultiWrite(i);
        }
        printf("\n\n");
    }
    return 0;
}*/

#include <iostream>
#include <condition_variable>
#include <mutex>
#include <thread>

std::condition_variable cv;
std::mutex mtx;
bool isReady = false;
bool workEnd = false;
void ThreadA() {

    while (!workEnd){
        std::unique_lock<std::mutex> lock(mtx);
        std::cout << "Thread A: Waiting for condition..." << std::endl;
        cv.wait(lock, [] { return isReady; });
        std::cout << "Thread A: Condition fulfilled. Continuing execution." << std::endl;
        isReady = false;
    }

}

void ThreadB() {
    std::this_thread::sleep_for(std::chrono::seconds(2));
    while (!workEnd){
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::unique_lock<std::mutex> lock(mtx);

        isReady = true;
        std::cout << "Thread B: Condition fulfilled. Notifying other threads." << std::endl;
        cv.notify_one();
    }
}

int main() {
    std::cout << "Main: Starting threads..." << std::endl;

    std::thread threadA(ThreadA);
    std::thread threadB(ThreadB);

    sleep(20);
    workEnd = true;

    threadA.join();
    threadB.join();


    std::cout << "Main: Threads finished." << std::endl;

    return 0;
}