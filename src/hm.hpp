//
// Created by workshop on 2/9/2022.
//
#ifndef HYBRIDMM_HM_HPP
#define HYBRIDMM_HM_HPP

#include <deque>
#include <unistd.h>
#include <libpmem.h>
#include <cstring>
#include "tool.h"
#include "threadPool.h"
#include <atomic>
#include <cstdint>
#include "config.h"
#include "index/utils.h"
#include <execinfo.h>
#define ALIGNED(N) __attribute__ ((aligned (N)))
#define CACHE_LINE_SIZE 64


//#define USE_HMM
//#define USE_NVM
#define MULTI_WRITE
//#define COMPRESS
//#define INVALID
//#define INVALID_PERSIST
//#define USE_Cache
//#define DO_OpLog
//#define globalReadOnly
//#define CHECHRes
//#define testGetNextAddress
//#define __DEBUG
//#define STATICS
//#define MEM_STAT
//#define NVM_ONLY

extern std::atomic<uint64_t> totalPage;

#define likely(x)       __builtin_expect((x), 1)
#define unlikely(x)     __builtin_expect((x), 0)

#ifndef value_size
#define value_size 8
#endif

#define NVMMetaFile "/optane/hmm/NVMMeta.bin"
#define OpMetaFile "/optane/hmm/OpMeta.bin"

#define NVMBlockFilePrefix "/optane/hmm/block"

#define FileNameMaxLen 64
#include <libpmemobj.h>
#define MEM_SIZE 400ul * 1024ul * 1024ul * 1024ul
const int  ARRAY_SIZE = 50000000L;

void init_nvmm_pool();

void *nvm_alloc(size_t size);

#define HMM_Alignment 64
typedef volatile u_int64_t LogAddress;
#define KeyType uint64_t
typedef struct ALIGNED(8) value_t{
    char value[value_size];
#ifdef DO_OpLog
    KeyType key;
    uint64_t timestamp;
#endif
    value_t(){
    }
    value_t(const int n){
    }
    value_t(volatile value_t &other){

        memcpy(value,  (void *)other.value, value_size);
#ifdef DO_OpLog
        key = other.key;
        timestamp = other.timestamp;
#endif
    }

    value_t(const value_t &other){

        memcpy(value,  (void *)other.value, value_size);
#ifdef DO_OpLog
        key = other.key;
        timestamp = other.timestamp;
#endif
    }
    value_t(value_t &other){
        memcpy(value,  (void *)other.value, value_size);
#ifdef DO_OpLog
        key = other.key;
        timestamp = other.timestamp;
#endif
    }

    volatile value_t& operator=( const volatile value_t &D ) volatile {
        memcpy((void *)value, (void *)D.value, value_size);
#ifdef DO_OpLog
        key = D.key;
        timestamp = D.timestamp;
#endif
        return *this;
    }
    value_t& operator=( value_t &D ){

        memcpy((void *)value, (void *)D.value, value_size);
#ifdef DO_OpLog
        key = D.key;
        timestamp = D.timestamp;
#endif
        return *this;
    }
    bool  operator==(const volatile value_t &D ){
#ifdef DO_OpLog
        return key == D.key && timestamp == D.timestamp;
#endif
        return  strncmp(value, (char *)D.value, value_size);
    }

    bool  operator==(const value_t &D ) const{
#ifdef DO_OpLog
        return key == D.key && timestamp == D.timestamp;
#endif
        return  strncmp(value, (char *)D.value, value_size);
    }

}value_t;

namespace std{
    template<>
    struct hash<value_t>: public __hash_base<size_t, value_t>{
        size_t operator() (const value_t& value) const noexcept{
            return std::hash<std::string>()(value.value);
        }
    };
}
typedef int fn_insert_t(void * index, KeyType key, value_t value);
typedef int fn_recovery_insert_t(void * index, KeyType key, LogAddress address);

typedef bool fn_update_t(key_t key, value_t value);
typedef void* fn_delete_t(key_t key);
typedef value_t* fn_get_t(key_t key);
typedef value_t* fn_scan_t(key_t startKey, int range);


//#define PageSizeBits 12 /*4k*/
//#define preparePersisThreshold (1<<14) /*16k*/
//#define freeSpaceThreshold (1<<14) /*16k*/
//#define BlockSizeBits 20 /*4k*/

#define AddressNil 0xFFFFFFFFFFFFFFFF
#define checkInvalidAddress(addr)  (addr==AddressNil)

//#define PageSizeBits 20
#define preparePersisPages (1<< 11)
#define pagesAfterReadOnly (1<<8)
//#define BlockSizeBits 30
#define InvalidBlockSizeBits 30
#define InvalidAddressCacheSizeBits 29
#ifndef dram_size
#define DRAM_TOTAL_SIZE (4L*1024*1024*1024L)
#else
#define DRAM_TOTAL_SIZE (1024L*1024*1024L*dram_size)
#endif
#define TOTAL_PAGE_NUM (DRAM_TOTAL_SIZE / (1<<PageSizeBits))
#define OpBlockSizeBits 30

#define PageArraySize TOTAL_PAGE_NUM

//#define PageSizeBits 20
//#define preparePersisThreshold (1L<<26)
//#define pagesAfterReadOnly (1<<4)
//#define BlockSizeBits 24
//#define InvalidBlockSizeBits 24
//#define InvalidAddressCacheSizeBits 8
//#define DRAM_TOTAL_SIZE (1L<<30)
//#define TOTAL_PAGE_NUM (1<<10)


#define MaxBlockNum 16*100
#define MaxPageNum 1024*1024
#define MaxThreadNum 64
#define MaxOpBlockNum 64

#define PageOffsetMask ((1L << PageSizeBits) - 1)
#define BlockOffsetMask ((1L << BlockSizeBits) - 1)
#define InvalidBlockOffsetMask ((1L << InvalidBlockSizeBits) - 1)
#define memoryUint sizeof(value_t)
//#define getValue(addr) __sync_fetch_and_add(&(addr), 0)
#define getValue(addr)  (addr)
#define getPageSize (1<< PageSizeBits)
#define getBlockSize (1<< BlockSizeBits)
#define persistPageNum   (1 << (BlockSizeBits - PageSizeBits))
#define getInvalidBlockSize ((sizeof(InvalidEntry)) << InvalidBlockSizeBits)
#define getOpBlockSize (1L << OpBlockSizeBits)

#define getIndex(addr, sizeBits) \
    (addr >> sizeBits)
#define getInnerOffset(addr, mask) \
    (addr & mask)
#define innerDRAMOffsetMax(dramWMeta) \
    ((dramWMeta->pageNum) << PageSizeBits)

#define getPageAddress(dataSpace, pageNo) \
    (dataSpace + (((u_int64_t)pageNo) << PageSizeBits))

#define getPageIndex(addr, hm)  \
    ((int)(((addr) >> PageSizeBits) % PageArraySize))

#define getInvalidAddressArrayNo(index)   \
    (((index) >> InvalidAddressCacheSizeBits) % 3)
#define getInvalidAddressArrayOffset(index)   \
    ((index) & ((1 << InvalidAddressCacheSizeBits)  - 1 ))
#define InvalidAddressCachePages \
    (1 << (InvalidAddressCacheSizeBits - PageSizeBits))

#define getOpBlockNo(pageNo) \
    ((pageNo >> (OpBlockSizeBits - PageSizeBits)) % MaxOpBlockNum)
#define getOpPageOff(pageNo) \
    ((pageNo%( 1 << (OpBlockSizeBits - PageSizeBits)))<< PageSizeBits)
#define getNVMFileName(filename,  blockNo) snprintf(filename, FileNameMaxLen, "%s%d.bin", NVMBlockFilePrefix, blockNo);
#define getNVMCompressFileName(filename,  blockNo, size) \
    snprintf(filename, FileNameMaxLen, "%s%d_%ld.bin", NVMBlockFilePrefix, blockNo, size);
#define getInvalidNVMFileName(filename,  blockNo, threadId) \
    snprintf(filename, FileNameMaxLen, "%sinvalid%d_%d.bin", NVMBlockFilePrefix, blockNo, threadId);
#define getOpBlockNVMFileName(filename,  blockNo) \
    snprintf(filename, FileNameMaxLen, "%sOp_%d.bin", NVMBlockFilePrefix, blockNo);


//#define pinPage(pageFlag) __sync_fetch_and_or(&(pageFlag), 1L << threadId)
//#define unPinPage(pageFlag) __sync_fetch_and_and(&(pageFlag), (~(1L << threadId)))
//#define pinPage(pageFlag)
//#define unPinPage(pageFlag)
#define pinPage(pageIndex, dramWMeta) dramWMeta->pageFlag[threadId][pageIndex >> 3] |= 1<<( pageIndex & 7)
#define unPinPage(pageIndex, dramWMeta) //dramWMeta->pageFlag[threadId][pageIndex >> 3] &= ~(1<<( pageIndex & 7))
#define getPageFlag(pageIndex, dramWMeta, threadId) (dramWMeta->pageFlag[threadId][pageIndex >> 3] & (1<<( pageIndex & 7)))

#define getActualDataAddress(dataAddress) ((char*)(((uint64_t)(dataAddress)) & ~(1L)))
#define setCompressFlag(dataAddress) ((char*)(((uint64_t)(dataAddress)) | (1L)))
#define getCompressFlag(dataAddress) ((char*)(((uint64_t)(dataAddress)) & (1L)))

#define setCacheFlag(dataAddress) ((LogAddress)(((uint64_t)(dataAddress)) | (1L)))
#define getCacheFlag(dataAddress) ((((uint64_t)(dataAddress)) & (1L)))
#define getActualAddress(dataAddress) ((char*)(((uint64_t)(dataAddress)) & ~(1L)))

#define CacheSampleFreq 100
#define CacheFreePageThreshold 10


#define checkFreq 100
#define deltaW 10
#define deltaR 4

#define SegFault  char* test = NULL; *test = 'c';

#define BACKTRACE_T void *buffer[16]; \
int stack_num = backtrace(buffer, 8); \
char **strings = backtrace_symbols(buffer, stack_num); \
for (int j = 0; j < stack_num; j++) \
fprintf(stderr, "(%ld) [%02d] %s\n", \
pthread_self(),  j, strings[j]); \
free(strings);

char* mallocNVM(const char* filename, size_t size);
char* openNVM(const char* filename, size_t size);

template<typename T>
class SafeQueue {
public:
    T q[TOTAL_PAGE_NUM];
    volatile int head;
    volatile int tail;
    SafeQueue(){
        head = tail = 0;
    }
    void Produce(T item) {
        int myHead = __sync_fetch_and_add(&head, 1);
        q[myHead % TOTAL_PAGE_NUM] = item;
    }
    bool Consume(T& item) {
        int myTail = tail;
        if(((myTail + 1) < head)
            && (__sync_bool_compare_and_swap (&tail, myTail, myTail + 1))){
                item = q[myTail % TOTAL_PAGE_NUM];
                return true;
        } else{
            return false;
        }

    }
};

typedef struct DRAMWriteRegionMeta{
public:
    int pageNum;
    int usedPages;
    int allocatedPages;
    std::atomic_int32_t writtenPages;
    int pages[TOTAL_PAGE_NUM];
    DRAMWriteRegionMeta(int startPage, int initPageCount){
        this->pageNum = initPageCount;
        this->usedPages = initPageCount;
        this->allocatedPages = initPageCount;
        this->writtenPages.store(0);
        for (int i = 0; i < this->allocatedPages; ++i) {
            pages[i] = startPage+i;
        }
    }

    void init(int startPage, int initPageCount){

    }
    ~DRAMWriteRegionMeta(){

    }
}DRAMWriteRegionMeta;

typedef struct ALIGNED(CACHE_LINE_SIZE) PerThreadNVMBlockMeta{
    int32_t deleted;
    int64_t lastDelete;
    int64_t oldLast;
    int64_t lastReorgnaize;
    PerThreadNVMBlockMeta(){
        init();
    }
    void init(){
        deleted = 0;
        lastDelete = -2;
        oldLast = lastDelete;
        lastReorgnaize = -2;
    }
}PerThreadNVMBlockMeta;

typedef struct NVMBlockMeta{
public:
    char* data;
    char* oldData;
    uint32_t size;
    int32_t blockNo;
    int32_t reorgnaized;
    char fileName[FileNameMaxLen];
    char oldName[FileNameMaxLen];
    PerThreadNVMBlockMeta perThreadMeta[MaxThreadNum];
    NVMBlockMeta(){
        init();
    }
    void init(){
        size = 1 << BlockSizeBits;
        reorgnaized = 0;
        oldData = NULL;
    }
}NVMBlockMeta;

/** for block compress   */
typedef struct HashBucket{
    LogAddress key;
    value_t value;
    int indexOfNext;
}HashBucket;
typedef struct HashMap{
    size_t num;
    HashBucket m_bucket[1];
}HashMap;

typedef struct NVMMeta{
public:
    union {
        struct {
            int blockCount;
            int currentBlockUsed;
        };
        char align[L1_CACHE_BYTES];
    };

    NVMBlockMeta blocks[MaxBlockNum];
    NVMMeta(){
#ifndef DO_OpLog
        init();
#endif
    }
    void init(){
        this->blockCount = 0;
        this->currentBlockUsed = 0;
        for (int i = 0; i < MaxBlockNum; ++i) {
            this->blocks[i].init();
        }
        this->blocks[0].blockNo = 0;
        getNVMFileName(blocks[0].fileName, 0);
        this->blocks[0].data = mallocNVM(blocks[0].fileName, getBlockSize);
    }
    ~NVMMeta(){
        for (int i = 0; i < this->blockCount; ++i) {
            if(blocks[i].size > 0){
                if(getCompressFlag(this->blocks[i].data)){
                    pmem_unmap(getActualDataAddress(this->blocks[i].data),
                               ((HashMap* )this->blocks[i].data)->num * sizeof (HashBucket) + sizeof (HashMap));
                } else{
                    pmem_unmap(getActualDataAddress(this->blocks[i].data), getBlockSize);
                }

            }

        }
    }
}NVMMeta;

typedef struct InvalidEntry{
    LogAddress address;
    int64_t     pre;
}InvalidEntry;

typedef struct InvalidBlock{
    char* data;
    InvalidBlock(){
        data = NULL;
    }
}InvalidBlock;

typedef struct InvalidAddressArray{
    int allocatedPages;
    std::atomic_int32_t writtenPages;
    int64_t persistedIndex;
    int64_t currentInnerBlockOff;
    InvalidBlock blocks[MaxBlockNum];
    __attribute__((aligned(HMM_Alignment))) InvalidEntry
            array[3][1 <<InvalidAddressCacheSizeBits];
    InvalidAddressArray(){
        persistedIndex = 0;
        currentInnerBlockOff = 0;
        writtenPages.store(0);
        allocatedPages = 3 * InvalidAddressCachePages;
    }
}InvalidAddressArray;

typedef struct CacheEntry{
    LogAddress logAddress;
    LogAddress *addressPtr;
    value_t     value;
}CacheEntry;

#define getCurrInnerOffset(addr) (addr & 0x00000000FFFFFFFF)
#define getCurrPage(addr) (addr >> 32)

typedef struct CacheRegionMeta{
    int pageNum;
    int usedPages;
    int allocatedPages;
    int dropPages;
    std::atomic_int32_t writtenPages;
    int pages[TOTAL_PAGE_NUM];
    CacheRegionMeta(int startPage, int initPageCount){
        this->pageNum = initPageCount;
        this->usedPages = initPageCount;
        this->writtenPages.store(0);
        this->allocatedPages = initPageCount;
        this->dropPages = 0;
        for (int i = 0; i < this->allocatedPages; ++i) {
            pages[i] = (i + startPage);
        }
    }
}CacheRegionMeta;


typedef struct HybridMemory HybridMemory;

typedef struct ALIGNED(CACHE_LINE_SIZE) NVMWriteAttribute{
    HybridMemory* hm;
    int threadId;
    int pageIndex;
    int pageNum;
    char* dest;
    bool needWrite;
    std::mutex m;
    std::condition_variable conditionVariable;
}NVMWriteAttribute;

typedef struct OpLogEntry{
    value_t  value;
}OpLogEntry;
typedef struct OpBlock{
    char *data;
    std::atomic_int64_t maxAddress;
    int fileNo;
    OpBlock(){
        data = NULL;
        maxAddress.store(0);
    }
}OpBlock;

typedef struct OperationLog{
    std::atomic_int32_t writtenPages;
    int32_t allocatedPages;
    int32_t miniOpBlock;
    OpBlock opBlocks[MaxOpBlockNum];
    OperationLog(){
        init();
    }
    void init(){
        writtenPages.store(0);
        miniOpBlock = 0;
        for (int i = 0; i < MaxOpBlockNum; ++i) {
            char fileName[FileNameMaxLen];
            getOpBlockNVMFileName(fileName, i);
            opBlocks[i].fileNo = i;
            opBlocks[i].data = mallocNVM(fileName, getOpBlockSize);
            memset(opBlocks[i].data, 0, getOpBlockSize);

        }
        allocatedPages = MaxOpBlockNum * getOpBlockSize / getPageSize;
    }
}OperationLog;


void free_thread(HybridMemory* hm);
void persist_thread(HybridMemory *hm);
void invalid_address_thread(HybridMemory* hm);
void read_cache_thread(HybridMemory* hm);
void NVMWriteThread(NVMWriteAttribute* attribute);
void compress_thread(HybridMemory* hm);
void freeNVM(char* data, size_t size, const char* filename);
void hm_timer(HybridMemory* hm);
extern thread_local int threadId;
void free_op_thread(HybridMemory* hm);

typedef struct ALIGNED(CACHE_LINE_SIZE) PerThreadMeta{
    uint64_t   readCount;
    uint64_t writeCount;
    uint64_t writeCurr; //page:32, off:32
    uint64_t invalidCurr; //page:32, off:32
    uint64_t cacheCurr; //page:32, off:32
    char*  cachePage;
    uint64_t OpCurr;  //page:32, off:32
    LogAddress OpCurrMaxAddress;
    uint32_t readOnlyPages;
    PerThreadMeta(){
        readOnlyPages = 0;
        readCount = 0;
        writeCount = 0;
        writeCurr = getPageSize - 1;
        invalidCurr = getPageSize;
        cacheCurr = getPageSize - 1;
        OpCurr = getPageSize - 1;
        OpCurrMaxAddress = 0;
        cachePage = NULL;
    }
    ~PerThreadMeta(){
    }
} PerThreadMeta;
void invalidAddress(LogAddress address, HybridMemory* hm);
bool hm_recovery(HybridMemory* hm, fn_insert_t insert, fn_recovery_insert_t recoveryInsert, void * index);
extern thread_local uint64_t waste;
#define MAX_BUCKET (99999991)

#define TEMP_ENTRIES_PER_BUCKET 3

typedef struct TempHashBucket{
    KeyType key[TEMP_ENTRIES_PER_BUCKET];
    uint64_t timestamp[TEMP_ENTRIES_PER_BUCKET];
    LogAddress address[TEMP_ENTRIES_PER_BUCKET];
    int num;
    struct TempHashBucket* next;
}TempHashBucket;
struct HybridMemory {
#ifdef globalReadOnly
    std::atomic_uint64_t readOnlyOffset;
#endif
    union {
        struct {
            DRAMWriteRegionMeta* dramWriteRegionMeta;
            NVMMeta* nvmMeta;
            CacheRegionMeta* cacheRegionMeta;
            InvalidAddressArray* invalidAddressArray;
            OperationLog* operationLog;
            char* dataSpace;
            uint64_t pagesBeforeReadOnly;
            uint32_t safeForFreePages;
            uint32_t globalDRAMPages;
        };
        uint8_t  padding1[CACHE_LINE_SIZE];
    };
    bool use_lock;
    bool is_writting;
    pthread_spinlock_t *spinlocks;
    TempHashBucket* tempHashtable;
    int persistThreadNum;
    int clientThreadMaxNum;
    std::atomic_int32_t clientThreadNum;
    bool workEnd;

    std::atomic_bool clientLive[MaxThreadNum];
    PerThreadMeta perThreadMeta[MaxThreadNum];
    pthread_t NVMWriteThreads[MaxThreadNum];
    volatile NVMWriteAttribute nvmWriteAttributes[MaxThreadNum];
    SafeQueue<int>  freePages;
    pthread_t persistThread;
    pthread_t compressThread;
    pthread_t freeThread;
    pthread_t invalidAddressThread;
    pthread_t readCacheThread;
#ifdef DO_OpLog
    pthread_t timerThread;
    pthread_t opThread;
#endif
    HybridMemory(int clientThreadMaxNum, double writeRegionRatio, bool use_lock,
                 fn_insert_t* insert, fn_recovery_insert_t* recoveryInsert, void *index):
    clientThreadMaxNum(clientThreadMaxNum){
        posix_memalign((void**)(&dataSpace), HMM_Alignment, DRAM_TOTAL_SIZE);
        memset(dataSpace, 0, DRAM_TOTAL_SIZE);
//        dataSpace = (char*)malloc(DRAM_TOTAL_SIZE);
        this->globalDRAMPages = 0;
        this->safeForFreePages = 0;
        this->use_lock = use_lock;
        this->is_writting = true;
        if(use_lock){
            spinlocks = static_cast<pthread_spinlock_t *>(malloc(MAX_BUCKET * sizeof(pthread_spinlock_t)));
            for (int i = 0; i < MAX_BUCKET; ++i) {
                pthread_spin_init(spinlocks + i, NULL);
            }
        } else{
            spinlocks = NULL;
        }
#ifdef globalReadOnly
        this->readOnlyOffset.store(0);
#endif
#ifdef DO_OpLog
        this->nvmMeta = (NVMMeta*) openNVM(NVMMetaFile, sizeof(NVMMeta));
        this->operationLog = (OperationLog*) openNVM(OpMetaFile, sizeof(OperationLog));

        pthread_create(&this->timerThread, NULL,
                       (void *(*)(void *))(hm_timer), (void *) this);
        pthread_create(&this->opThread, NULL,
                       (void *(*)(void *))(free_op_thread), (void *) this);
#else
        this->nvmMeta = new NVMMeta();
#endif
        this->dramWriteRegionMeta = new DRAMWriteRegionMeta(0, writeRegionRatio * TOTAL_PAGE_NUM);
        this->cacheRegionMeta = new CacheRegionMeta(writeRegionRatio * TOTAL_PAGE_NUM,
                                                    TOTAL_PAGE_NUM - writeRegionRatio * TOTAL_PAGE_NUM);
        this->invalidAddressArray = new InvalidAddressArray();

        this->workEnd = false;
        this->persistThreadNum = 7;
        this->pagesBeforeReadOnly =
                (((uint64_t)dramWriteRegionMeta->usedPages) > preparePersisPages) ?
                (((uint64_t)dramWriteRegionMeta->usedPages - preparePersisPages)) :
                1;

        for (int i = 0; i < clientThreadMaxNum; ++i) {
            clientLive[i].store(false);
        }

#ifndef testGetNextAddress
        pthread_create(&this->persistThread, NULL,
                       (void *(*)(void *))(persist_thread), (void *) this);
        pthread_create(&this->freeThread, NULL,
                       (void *(*)(void *))(free_thread), (void *) this);

#endif
#ifdef COMPRESS
        pthread_create(&this->compressThread, NULL,
                       (void *(*)(void *))(compress_thread), (void *) this);
#endif
#ifdef INVALID_PERSIST
        pthread_create(&this->invalidAddressThread, NULL,
                        (void *(*)(void *))(invalid_address_thread), (void *) this);
#endif

#ifdef USE_Cache
        pthread_create(&this->readCacheThread, NULL,
                       (void *(*)(void *))(read_cache_thread), (void *) this);
#endif
#ifdef MULTI_WRITE
        for (int i = 0; i < persistThreadNum; ++i) {
            nvmWriteAttributes[i].hm = this;
            nvmWriteAttributes[i].threadId = i;
            nvmWriteAttributes[i].needWrite= false;
            pthread_create(this->NVMWriteThreads + i, NULL,
                           (void *(*)(void *))(NVMWriteThread), (void *)(nvmWriteAttributes + i));
        }
#endif
    }
    ~HybridMemory(){
        vmlog(LOG, "start HybridMemory destory");
        this->workEnd = true;

#ifndef testGetNextAddress
        pthread_join(this->persistThread, NULL);
        pthread_join(this->freeThread, NULL);
#endif


#ifdef COMPRESS
        pthread_join(this->compressThread, NULL);
#endif
#ifdef INVALID_PERSIST
        pthread_join(this->invalidAddressThread, NULL);
#endif
#ifdef USE_Cache
        pthread_join(this->readCacheThread, NULL);
#endif

#ifdef MULTI_WRITE
        for (int i = 0; i < persistThreadNum; ++i) {
            ((NVMWriteAttribute*)(nvmWriteAttributes + i))->conditionVariable.notify_one();
            pthread_join(this->NVMWriteThreads[i], NULL);
        }
#endif
        delete this->dramWriteRegionMeta;
#ifdef DO_OpLog
        pthread_join(this->timerThread, NULL);
        pthread_join(this->opThread, NULL);
        freeNVM(reinterpret_cast<char *>(this->nvmMeta), sizeof(NVMMeta), NVMMetaFile);
        freeNVM((char *)(this->operationLog), sizeof(OperationLog), OpMetaFile);
#else
        delete this->nvmMeta;
#endif
        delete this->cacheRegionMeta;
        free(dataSpace);
        if(this->use_lock){
            free((void *) this->spinlocks);
        }
        vmlog(LOG, "end HybridMemory destory");
    }

    void clientEnd(){
        clientLive[threadId].store(false);
        waste += (getPageSize - getCurrInnerOffset(perThreadMeta[threadId].writeCurr));
        LogAddress wasteAddress = 0;
        while ((getCurrInnerOffset(perThreadMeta[threadId].writeCurr) + memoryUint) < getPageSize){
            LogAddress address = getCurrPage(perThreadMeta[threadId].writeCurr) * getPageSize
                    + getCurrInnerOffset(perThreadMeta[threadId].writeCurr);
            invalidAddress(address, this);
            perThreadMeta[threadId].writeCurr += memoryUint;
            wasteAddress = address;
        }
        perThreadMeta[threadId].writeCurr = getPageSize - 1;
        perThreadMeta[threadId].readOnlyPages = 0;
        clientThreadNum.fetch_sub(1);
        vmlog(LOG, "clientEnd, thread:%d, clientThreadNum:%d, wasteAddress:%ld",
              threadId, clientThreadNum.load(), wasteAddress);
    }
    void initClient(int threadNum){
        clientThreadNum.store(threadNum);
        for (int i = 0; i < threadNum; ++i) {
            clientLive[i].store(true);
            perThreadMeta[i].readOnlyPages = safeForFreePages;
            perThreadMeta[i].readCount = 0;
            perThreadMeta[i].writeCount = 0;
            perThreadMeta[i].writeCurr = getPageSize - 1;
            perThreadMeta[i].cacheCurr = getPageSize - 1;
            perThreadMeta[i].OpCurr = getPageSize - 1;
            perThreadMeta[i].OpCurrMaxAddress = 0;
        }
    }
};

value_t* hm_insert(HybridMemory* hm, key_t startKey, int range, fn_insert_t* func);
bool hm_update(HybridMemory* hm, key_t key, value_t value, fn_update_t* func);
value_t hm_delete(HybridMemory* hm, key_t key, fn_delete_t* func);
value_t* hm_get(HybridMemory* hm, key_t key, fn_get_t* func);
value_t* hm_scan(HybridMemory* hm, key_t startKey, int range, fn_scan_t* func);

void* hm_alloc(size_t size);
void hm_free(void *addr);

value_t *get_value(HybridMemory* hm, LogAddress addr);

value_t *get_value_for_read(HybridMemory* hm, LogAddress &addr);

void set_value(HybridMemory* hm, LogAddress &oldAddress, KeyType key, value_t* newValue);

void nt_store(char* dest, char* src, size_t len);

#endif //HYBRIDMM_HM_HPP
