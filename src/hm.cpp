//
// Created by workshop on 2/9/2022.
//

#include <cstdio>
#include <iostream>
#include <cassert>
#include "hm.hpp"
#include <sched.h>
#include <errno.h>
#include <mmintrin.h>  //MMX
#include <immintrin.h>  // _mm_clflushopt _mm512_stream_si512 _mm512_load_epi32
#include "index/utils.h"
thread_local int threadId;
std::atomic<uint64_t> totalPage(0) ;

#define usleepTime 50
#define prime 43112609

volatile uint64_t timestamp = 0;

double recoveryTime = 0;

bool do_recovery = true;
bool recovery_nvm_block = false;
uint64_t dram_hit[MaxThreadNum];

static inline void __attribute__((__always_inline__)) smp_mb(void)
{
    __asm__ __volatile__("mfence" ::: "memory");
}


char* mallocNVM(const char* filename, size_t size){
    char* str;
#ifdef USE_NVM
    if(access(filename, F_OK) == 0){
        remove(filename);
    }
    size_t mapped_len;
    int is_pmem = 1;
    str = (char*) pmem_map_file(filename, size,
                                PMEM_FILE_CREATE|PMEM_FILE_EXCL,
                                0666, &mapped_len, &is_pmem);
    if(str == NULL){
        vmlog(ERROR, "mallocNVM pmem_map_file:%s error. size:%ld, mapped_len:%ld, is_pmem:%d, errno:%d  ",
              filename, size, mapped_len, is_pmem, errno);
    }
    vmlog(DEBUG, "mallocNVM pmem_map_file:%s ", filename);
#else
    posix_memalign((void**)(&str), HMM_Alignment, size);
#endif
    return str;
}

char* openNVM(const char* filename, size_t size){
    char* str;
#ifdef USE_NVM
    size_t mapped_len;
    int is_pmem = 1;
    if(access(filename, F_OK) == 0){
        str = (char*) pmem_map_file(filename, size,
                                    PMEM_FILE_CREATE,
                                    0666, &mapped_len, &is_pmem);
        if(str == NULL){
            vmlog(ERROR, "openNVM  exist pmem_map_file:%s error. size:%ld, mapped_len:%ld, is_pmem:%d, errno:%d  ",
                  filename, size, mapped_len, is_pmem, errno);
        } else{
            vmlog(DEBUG, "openNVM  exist pmem_map_file:%s ", filename);
        }
    } else{

        str = (char*) pmem_map_file(filename, size,
                                    PMEM_FILE_CREATE|PMEM_FILE_EXCL,
                                    0666, &mapped_len, &is_pmem);
        if(str == NULL){
            vmlog(ERROR, "openNVM new pmem_map_file:%s error. size:%ld, mapped_len:%ld, is_pmem:%d, errno:%d  ",
                  filename, size, mapped_len, is_pmem, errno);
        }
        memset(str, 0, size);

    }


#else
    posix_memalign((void**)(&str), HMM_Alignment, size);
#endif
    return str;
}

void freeNVM(char* data, size_t size, const char* filename){
    pmem_unmap(data, size);
    if(access(filename, F_OK) == 0){
        remove(filename);
    }
}

//len should be multiple of 64
inline void nt_store(char* dest, char* src, size_t len){
    uint64_t j = 0;
    for (; j < len; j+=(512 >> 3)) {
        _mm512_stream_si512((__m512i*)(dest + j), *(__m512i*)(src +j));
    }
    if(j != len){
        j -=(512 >> 3);
        memcpy(dest + j, src + j, (len - j));
    }
}

inline InvalidEntry getInvalidEntry(InvalidAddressArray* invalidAddressArray, int64_t index){
    int blockNo = (int)(index >> InvalidBlockSizeBits);
    while (true){
        if(index < invalidAddressArray->persistedIndex){
//            vmlog(LOG, "getInvalidEntry, from NVM: index:%ld",index);
            return *(((InvalidEntry*)(invalidAddressArray->blocks[blockNo].data))
            + getInnerOffset(index, InvalidBlockOffsetMask));
        } else{
            InvalidEntry entry =
                    (invalidAddressArray->array[getInvalidAddressArrayNo(index)][getInvalidAddressArrayOffset(index)]);
            if(index >= invalidAddressArray->persistedIndex){
                return entry;
            }
        }
    }
}


#define  intkey_t LogAddress
#define h1 2
inline intkey_t hashKey(const intkey_t k) {
    intkey_t  h= k;
    for(int i = 0; i < (int)sizeof(k); i+= 16){
        short s = h & ((0xFFF) << i);
        s = s *( s + h1);
        h = (h & (~((0xFFF) << i))) | (s << i);
    }
    return k;
}

#define hash(key, bucketSize) ((key%prime)%bucketSize)

// no duplicate key
inline void compressHashMapPut(HashBucket* m_bucket, bool* bucketUsed, size_t bucketSize, LogAddress key, value_t* value, int &cursor){
#ifdef CHECHRes
    if(value->value[0] == 0){
        vmlog(DEBUG, "compressHashMapPut: address:%ld, value:0", key);
    }
#endif
    int index = hash(key, bucketSize);

    // If the location is null, insert a new bucket to hashmap
    if(!bucketUsed[index]) {
        m_bucket[index].key = key;
        m_bucket[index].value = *value;
        bucketUsed[index] = true;
        return ;
    }

    // Find the right location in the cellar for this new value, starting at (tablesize - 1)
    int depth = 0;
    while ( cursor >= 0 && bucketUsed[cursor]) {
        --cursor;
        depth ++;
    }

    // Table is full, so return failure
    if ( cursor < 0 ) {
        vmlog(ERROR, "bucket full during compress");
        return;
    }

    // Insert new bucket at the cursor location
    m_bucket[cursor].key = key;
    m_bucket[cursor].value = *value;
    bucketUsed[cursor] = true;

    // Point the colliding node to this new node
    while ( m_bucket[index].indexOfNext != -1 ) {
        if(m_bucket[index].indexOfNext < 0){
            vmlog(ERROR, "compressHashMapPut: key:%d, index:%d, indexOfNext:%d",
                  key, index, m_bucket[index].indexOfNext );
        }
        index = m_bucket[index].indexOfNext;
        depth ++;
    }

    m_bucket[index].indexOfNext = cursor;
    vmlog(DEBUG, "key:%ld, index: %d, indexOfNext :%d, depth:%d", key, index, cursor, depth);

}

inline char* compressHashMapGet(HashBucket* m_bucket, size_t bucketSize, LogAddress key){
    int index = hash(key, bucketSize);

    while (index != -1 && m_bucket[index].key != key) {
        index = m_bucket[index].indexOfNext;
    }

    if (index == -1) {
        vmlog(ERROR, "compressHashMapGet not found :%ld, block:%d", key, getIndex(key, BlockSizeBits));
        return NULL;
    }
#ifdef CHECHRes
    if(m_bucket[index].value.value[0] == 0){
        vmlog(DEBUG, "compressHashMapGet: address:%ld, value:0", key);
    }
#endif

    return (char*)(&m_bucket[index].value);
}

inline void compressHashMapInit(HashMap* hashMap){
    for (size_t i = 0; i < hashMap->num; ++i) {
        hashMap->m_bucket[i].indexOfNext = -1;
    }
}

inline bool compressBlock(NVMBlockMeta* blockMeta, HybridMemory* hm, int totalDelete){
    if((do_recovery) || (blockMeta->size != (1<<BlockSizeBits))){
        return false;
    }

    uint32_t newSize = ((int)blockMeta->size > totalDelete)? blockMeta->size -totalDelete : 0;
    if(newSize == 0){
        if((int)blockMeta->size < totalDelete){
            vmlog(WARN, "compressBlock totalDelete:%d > blockMeta->size:%ld",
                  totalDelete, blockMeta->size);
        }
        blockMeta->size = 0;
        return false;
    }
    LogAddress startAddress = ((uint64_t)blockMeta->blockNo) << BlockSizeBits;

    uint32_t remainTupleNum = newSize / memoryUint + MaxThreadNum;
    bool* bucketUsed = new bool[remainTupleNum];
    char* valueFlag = new char[blockMeta->size >> 3];
    memset(bucketUsed, 0, sizeof (bool) * remainTupleNum);
    memset(valueFlag, 0, sizeof (char) * (blockMeta->size >> 3));
    int labelDelete = 0;
    for (int i = 0; i < hm->clientThreadMaxNum; ++i) {
        int64_t lastDelete = blockMeta->perThreadMeta[i].oldLast;
        int perThreadLabelDelete = 0;
        int retry = 0;
        while (lastDelete != blockMeta->perThreadMeta[i].lastReorgnaize){
            if(lastDelete < 0){
                vmlog(ERROR, "index error: compressBlock:%d, "
                             "lastDelete:%ld, persistIndex:%ld, thread:%d",
                             blockMeta->blockNo, lastDelete,
                             hm->invalidAddressArray->persistedIndex, i);
            }
            InvalidEntry invalidEntry = getInvalidEntry(hm->invalidAddressArray, lastDelete);
//            if(invalidEntry.address == 0 && invalidEntry.pre ==0){
//                vmlog(WARN, "compressBlock:%d, address:%ld, pre:%ld, "
//                            "lastDelete:%ld, persistIndex:%ld, thread:%d",
//                            blockMeta->blockNo, invalidEntry.address, invalidEntry.pre, lastDelete,
//                            hm->invalidAddressArray->persistedIndex, i);
//            }
            if((int32_t)(invalidEntry.address >> BlockSizeBits) != blockMeta->blockNo){
                if(retry == 10000){
                    return false;
//                    vmlog(ERROR, "not belong to this block, compressBlock:%d, address:%ld, pre:%ld, "
//                                "lastDelete:%ld, persistIndex:%ld, thread:%d",
//                                blockMeta->blockNo, invalidEntry.address, invalidEntry.pre, lastDelete,
//                                hm->invalidAddressArray->persistedIndex, i);
//                    SegFault
                }
                retry ++;
                continue;
            }
            int index = (int)(invalidEntry.address - startAddress);
            vmlog(DEBUG, "invalidEntry:(%ld,%ld,%ld, %d)",
                  invalidEntry.address, lastDelete, invalidEntry.pre, index);
            if((valueFlag[index >> 3] & (1 << (index &7)))){
                vmlog(DEBUG, "duplicate label!!!!! block:%d, address:%ld, index:%d",
                      blockMeta->blockNo, invalidEntry.address, index);
            }
            valueFlag[index >> 3] |= (1 << (index & 7));
            lastDelete = invalidEntry.pre;
            perThreadLabelDelete++;
        }
        vmlog(DEBUG, "compressBlock:%d, totalDelete:%d, perThreadLabelDelete:%d",
              blockMeta->blockNo, totalDelete, perThreadLabelDelete);
        labelDelete += perThreadLabelDelete;
    }

    if(((labelDelete + MaxThreadNum) * (int)memoryUint) < totalDelete){
        for (int i = 0; i < hm->clientThreadMaxNum; ++i) {
//            vmlog(LOG, "compressBlock:%d, thread:%d, deleted:%d",
//                  blockMeta->blockNo, i, blockMeta->perThreadMeta[i].deleted);
        }
        vmlog(ERROR, "compressBlock:%d, totalDelete:%d, labelDelete:%d",
              blockMeta->blockNo, totalDelete, labelDelete);
    }
    vmlog(WARN, "compressBlock:%d remainTupleNum:%d, labelDelete:%d, totalDelete:%d",
          blockMeta->blockNo, remainTupleNum, labelDelete, totalDelete);
    HashMap* hashMap;
    size_t dataSize = remainTupleNum * sizeof (HashBucket) + sizeof (HashMap);
    posix_memalign((void**)(&hashMap), HMM_Alignment, dataSize);
    hashMap->num = remainTupleNum;
    compressHashMapInit(hashMap);
    int actualDelete = 0;
    int inserted = 0;
    int cursor = remainTupleNum - 1;
    for (LogAddress i = 0; i < blockMeta->size; i += getPageSize) {
        for (LogAddress j = 0; (j + memoryUint) <= getPageSize; j += memoryUint) {
            LogAddress address = i + j;
            if(!(valueFlag[address >> 3] & (1 << (address &7)))){
                inserted ++;
                vmlog(DEBUG, "compressBlock: blockNo:%d, remainTupleNum:%d, totalDelete:%d,"
                           " labelDelete:%d, actualDelete:%d, inserted:%d, address:%ld",
                           blockMeta->blockNo, remainTupleNum, totalDelete, labelDelete,
                           actualDelete, inserted, (startAddress + address));
                compressHashMapPut(hashMap->m_bucket, bucketUsed, remainTupleNum,
                                   (LogAddress)(startAddress + address),
                                   (value_t*)(getActualDataAddress(blockMeta->data) + address), cursor);

            } else{
                vmlog(DEBUG, "compressBlock: blockNo:%d, remainTupleNum:%d, totalDelete:%d,"
                           " labelDelete:%d, actualDelete:%d, inserted:%d,  address:%ld",
                           blockMeta->blockNo, remainTupleNum, totalDelete, labelDelete, actualDelete, inserted, address);
                actualDelete ++;
            }

        }

    }
    vmlog(DEBUG, "compressBlock: actualDelete:%d", actualDelete);
    memcpy(blockMeta->oldName, blockMeta->fileName, FileNameMaxLen);
#ifdef DO_OpLog
    flush_to_nvm(blockMeta->oldName, FileNameMaxLen);
#endif
    blockMeta->oldData = blockMeta->data;
    getNVMCompressFileName(blockMeta->fileName, blockMeta->blockNo, (dataSize >> 20));
    char* newData = mallocNVM(blockMeta->fileName, dataSize);
    nt_store(newData, (char*)hashMap, dataSize);
    blockMeta->size = dataSize;
    smp_mb();
    blockMeta->data = setCompressFlag(newData);
    vmlog(WARN, "compressBlock: %d, actualDelete:%d, dataSize:%d",
          blockMeta->blockNo, actualDelete, dataSize);
    blockMeta->reorgnaized += memoryUint*actualDelete;
#ifdef DO_OpLog
    flush_to_nvm(blockMeta, sizeof (NVMBlockMeta));
    smp_mb();
#endif
    delete[] bucketUsed;
    delete[] valueFlag;
    return true;
}

void compress_thread(HybridMemory* hm){
    NVMMeta* nvmMeta = hm->nvmMeta;
    while (!hm->workEnd){
        bool compress = false;
        for (int i = 0; i < (nvmMeta->blockCount - 2) && (!hm->workEnd); ++i) {
            if( nvmMeta->blocks[i].oldData != NULL){
                freeNVM( nvmMeta->blocks[i].oldData, getBlockSize, nvmMeta->blocks[i].oldName);
                nvmMeta->blocks[i].oldData = NULL;
            }
            int totalDelete = 0;
            for (int j = 0; j < MaxThreadNum; ++j) {
                totalDelete += nvmMeta->blocks[i].perThreadMeta[j].deleted;
            }
            totalDelete -= nvmMeta->blocks[i].reorgnaized;
            if(totalDelete > 0){
                vmlog(DEBUG, "block:%d, deleted:%d", i, totalDelete);
            }
            if(totalDelete >= (1 <<(BlockSizeBits - 1))){
                compress = compressBlock(nvmMeta->blocks + i, hm, totalDelete);
            }
        }
        if(!compress){
            usleep(usleepTime);
        }
    }
    vmlog(LOG, "compress_thread end");
}

inline void persist_invalid_address(InvalidAddressArray* addressArray){
    int blockNo = (int)(addressArray->persistedIndex >> InvalidBlockSizeBits);
//    int persistArrayNo = getInvalidAddressArrayNo(addressArray->persistedIndex);
    uint64_t persistLen = sizeof(addressArray->array[0]);
    assert((addressArray->currentInnerBlockOff + persistLen) <= getInvalidBlockSize);
    if(addressArray->blocks[blockNo].data == NULL){
        char filename[FileNameMaxLen];
        getInvalidNVMFileName(filename, blockNo, 0);
        addressArray->blocks[blockNo].data = mallocNVM(filename, getInvalidBlockSize);
        vmlog(LOG, "persist_invalid_address, new file:%s", filename);
    }
    vmlog(LOG, "persist_invalid_address, persistLen :%ld", persistLen);
    nt_store((char*)addressArray->blocks[blockNo].data + addressArray->currentInnerBlockOff,
             (char*)addressArray->array[getInvalidAddressArrayNo(addressArray->persistedIndex)], persistLen);
    addressArray->persistedIndex += (1 <<InvalidAddressCacheSizeBits);
    addressArray->allocatedPages += InvalidAddressCachePages;
    addressArray->currentInnerBlockOff = ((addressArray->currentInnerBlockOff + persistLen) >= (int64_t)getInvalidBlockSize)?
            0: (addressArray->currentInnerBlockOff + (int64_t)persistLen);
    vmlog(LOG, "persist_invalid_address end, allocatedPages:%d, currentInnerBlockOff:%ld",
          addressArray->allocatedPages, addressArray->currentInnerBlockOff);
}
thread_local uint64_t invalidWhileCount = 0;

inline void getNextInvalidPageForThread(HybridMemory* hm){
    if(getCurrInnerOffset(hm->perThreadMeta[threadId].invalidCurr) < getPageSize){
        vmlog(LOG, "invalid Page waste:%ld",
              (getPageSize - getCurrInnerOffset(hm->perThreadMeta[threadId].invalidCurr)));
    }
    uint64_t currPage = hm->invalidAddressArray->writtenPages.fetch_add(1);
    hm->perThreadMeta[threadId].invalidCurr = ((currPage) << 32);
#ifdef INVALID_PERSIST
    while((currPage) >= (uint64_t)hm->invalidAddressArray->allocatedPages){
        invalidWhileCount ++;
        vmlog(LOG, "wait for invalidPage:%ld: allocatedPages:%d, persist:%ld",
              currPage, hm->invalidAddressArray->allocatedPages,
              hm->invalidAddressArray->persistedIndex >> PageSizeBits);
    }
#endif
    vmlog(DEBUG, "getNextInvalidPageForThread thread:%d, page:%ld", threadId, currPage);
}

int count = 0;
void invalidAddress(LogAddress address, HybridMemory* hm){
retry:
    if(address == AddressNil){
        return;
    }
    uint64_t pageNo = getCurrPage(hm->perThreadMeta[threadId].invalidCurr);
    uint64_t innerOff = getCurrInnerOffset(hm->perThreadMeta[threadId].invalidCurr);
    if((innerOff >= getPageSize)
    || ((((hm->invalidAddressArray->writtenPages >= (hm->invalidAddressArray->allocatedPages - InvalidAddressCachePages))))
        && (pageNo < ((uint64_t)(hm->invalidAddressArray->persistedIndex >> PageSizeBits) + InvalidAddressCachePages)))){
        getNextInvalidPageForThread(hm);
        goto retry;
    }
    PerThreadNVMBlockMeta* threadMeta =  hm->nvmMeta->blocks[getIndex(address, BlockSizeBits)].perThreadMeta + threadId;
    uint64_t index = (pageNo << PageSizeBits) + innerOff;
    hm->invalidAddressArray->array[getInvalidAddressArrayNo(index)][getInvalidAddressArrayOffset(index)]
        = {address, threadMeta->lastDelete};
    if(address == 0 && threadMeta->lastDelete == 0){
        vmlog(WARN, "invalidAddress:(%ld,%ld,%ld)", address, index, threadMeta->lastDelete);

    }
    threadMeta->oldLast = threadMeta->lastDelete;
    threadMeta->lastDelete = index;
    threadMeta->deleted += memoryUint;
    hm->perThreadMeta[threadId].invalidCurr ++;
}
thread_local uint64_t whileCount = 0;
thread_local uint64_t waste = 0;
thread_local uint64_t newAddress = 0;
inline void checkReadOnlyOffset(HybridMemory* hm);
inline void getNextWritePageForThread(HybridMemory* hm){
    uint64_t  waste = 0;
    while ((getCurrInnerOffset(hm->perThreadMeta[threadId].writeCurr)  + memoryUint)< getPageSize){
        LogAddress address = getCurrPage(hm->perThreadMeta[threadId].writeCurr) * getPageSize
                + getCurrInnerOffset(hm->perThreadMeta[threadId].writeCurr);
        invalidAddress(address, hm);
        hm->perThreadMeta[threadId].writeCurr += memoryUint;
        waste += memoryUint;
    }
    if(waste > 0){
        vmlog(WARN, "getNextWritePageForThread: page:%d, waste:%ld", getCurrPage(hm->perThreadMeta[threadId].writeCurr), waste );

    }
    uint64_t currPage = hm->dramWriteRegionMeta->writtenPages.fetch_add(1);
    hm->perThreadMeta[threadId].writeCurr = ((currPage) << 32);
    whileCount = 0;
    while((currPage) >= (uint64_t)hm->dramWriteRegionMeta->allocatedPages){
        whileCount ++;
        if(((hm->dramWriteRegionMeta->writtenPages.load() > (int32_t)hm->perThreadMeta[threadId].readOnlyPages))
        && (hm->dramWriteRegionMeta->writtenPages.load() - (int32_t)hm->perThreadMeta[threadId].readOnlyPages) > (int32_t)(hm->pagesBeforeReadOnly)){
            hm->perThreadMeta[threadId].readOnlyPages ++;
        }
        usleep(1000);
 #ifdef __DEBUG

        vmlog(LOG, "wait for page:%ld: globalDRAM:%d, "
                     "readOnly:%d, safeForFree:%d, "
                     "usedPages:%d, pageNum:%d, allocatedPages:%d",
                     currPage, hm->globalDRAMPages, hm->perThreadMeta[threadId].readOnlyPages,
                     hm->safeForFreePages, hm->dramWriteRegionMeta->usedPages,
                     hm->dramWriteRegionMeta->pageNum, hm->dramWriteRegionMeta->allocatedPages);
 #endif
    }
#ifdef __DEBUG
    vmlog(LOG, "getNextWritePageForThread thread:%d, page:%ld, readonly:%ld, beforeReadOnly:%ld",
          threadId, currPage,  hm->perThreadMeta[threadId].readOnlyPages, hm->pagesBeforeReadOnly);
#endif
}

inline void getNextCachePageForThread(HybridMemory* hm){
    if(do_recovery){
        return;
    }
    if ( (hm->perThreadMeta[threadId].cachePage != NULL) &&
        (getCurrInnerOffset(hm->perThreadMeta[threadId].cacheCurr)  + memoryUint)< getPageSize){
        uint64_t cacheOffset = getCurrInnerOffset(hm->perThreadMeta[threadId].cacheCurr);
        uint64_t currPage = (getCurrPage(hm->perThreadMeta[threadId].cacheCurr));
        CacheEntry* cacheEntry = (CacheEntry*)
                (getPageAddress(hm->dataSpace,hm->cacheRegionMeta->pages[currPage % PageArraySize])
                + cacheOffset);
        *cacheEntry = {.logAddress = 0, .addressPtr = NULL};
    }
    if(hm->cacheRegionMeta->writtenPages >= hm->cacheRegionMeta->allocatedPages){
        hm->perThreadMeta[threadId].cacheCurr = 0;
        hm->perThreadMeta[threadId].cachePage = NULL;
        return;
    }
    uint64_t currPage = hm->cacheRegionMeta->writtenPages.fetch_add(1);

    if(currPage >= hm->cacheRegionMeta->allocatedPages){
        hm->perThreadMeta[threadId].cacheCurr = 0;
        hm->perThreadMeta[threadId].cachePage = NULL;
        return;
    }
    hm->perThreadMeta[threadId].cacheCurr = ((currPage) << 32);
    hm->perThreadMeta[threadId].cachePage =
            getPageAddress(hm->dataSpace,hm->cacheRegionMeta->pages[currPage % PageArraySize]);
    vmlog(DEBUG, "getNextCachePageForThread: currPage:%d,  address:%lx",
          currPage, getPageAddress(hm->dataSpace,hm->dramWriteRegionMeta->pages[currPage % PageArraySize]));
#ifdef __DEBUG
    vmlog(DEBUG, "getNextCachePageForThread thread:%d, page:%ld", threadId, currPage);
#endif
}

inline void getNextOpPageForThread(HybridMemory* hm){
    uint64_t blockNo = getOpBlockNo(getCurrPage(hm->perThreadMeta[threadId].OpCurr));
    if ((getCurrInnerOffset(hm->perThreadMeta[threadId].OpCurr)  + memoryUint)< getPageSize){
        uint64_t innerOffset = getCurrInnerOffset(hm->perThreadMeta[threadId].OpCurr);
        OpLogEntry* opLogEntry = (OpLogEntry*)
                (hm->operationLog->opBlocks[blockNo].data + innerOffset);
#ifdef DO_OpLog
        opLogEntry->value.key = AddressNil;
#endif
    }
    while(hm->operationLog->opBlocks[blockNo].maxAddress.load() < (int64_t)hm->perThreadMeta[threadId].OpCurrMaxAddress){
        hm->operationLog->opBlocks[blockNo].maxAddress.store(hm->perThreadMeta[threadId].OpCurrMaxAddress);
    }
    uint64_t currPage = hm->operationLog->writtenPages.fetch_add(1);
    while((currPage) >= (uint64_t)hm->operationLog->allocatedPages){
        //TODO: wait for free OperationLog space
        hm->operationLog->allocatedPages += getOpBlockSize / getPageSize;
    }
    hm->perThreadMeta[threadId].OpCurr = ((currPage) << 32);
    hm->perThreadMeta[threadId].OpCurrMaxAddress = 0;
    flush_to_nvm((void *)hm->operationLog, 2 * sizeof(uint64_t));
    smp_mb();

}

thread_local int counter = 0;

typedef enum Operation{
    OP_WRITE,
    OP_READ
}Operation;

//#define MiniSpace (getBlockSize / getPageSize)
#define MiniSpace 2
#define checkInterval 100
inline void checkReadOnlyOffset(HybridMemory* hm, Operation op){
    counter ++;
    if( (op!= OP_WRITE) && (counter % checkInterval != 0)){
        return;
    }
#ifndef globalReadOnly
    if( unlikely( ((hm->dramWriteRegionMeta->writtenPages + (getBlockSize / getPageSize) ) >= (hm->dramWriteRegionMeta->allocatedPages))
        && (getCurrPage(hm->perThreadMeta[threadId].writeCurr) < (hm->safeForFreePages + (getBlockSize / getPageSize)))
        && hm->is_writting)){
#ifdef __DEBUG
        waste += (getPageSize - getCurrInnerOffset(hm->perThreadMeta[threadId].writeCurr));
        vmlog(LOG, "checkReadOnlyOffset, getNextWritePageForThread: waste:%ld, currPage:%ld, safeForFree:%d, allocatedPage:%ld",
              (getPageSize - getCurrInnerOffset(hm->perThreadMeta[threadId].writeCurr)),
              getCurrPage(hm->perThreadMeta[threadId].writeCurr), hm->safeForFreePages, hm->dramWriteRegionMeta->allocatedPages);
#endif
        getNextWritePageForThread(hm);
    }
    if(unlikely(((hm->dramWriteRegionMeta->writtenPages > (int32_t)hm->perThreadMeta[threadId].readOnlyPages))
    && (((hm->dramWriteRegionMeta->writtenPages - hm->perThreadMeta[threadId].readOnlyPages) > (hm->pagesBeforeReadOnly))))){
        hm->perThreadMeta[threadId].readOnlyPages ++;
    }
#endif
    if(unlikely(((hm->cacheRegionMeta->writtenPages + CacheFreePageThreshold) >= hm->cacheRegionMeta->allocatedPages ) &&
        ((int32_t)getCurrPage(hm->perThreadMeta[threadId].cacheCurr) <= (hm->cacheRegionMeta->dropPages + 1)))){
        getNextCachePageForThread(hm);
    }
}



void addOperationLog(HybridMemory* hm, LogAddress address, value_t* value, uint64_t ts){
    if(do_recovery){
        return;
    }
    retry:
    uint64_t innerOffset = getCurrInnerOffset(hm->perThreadMeta[threadId].OpCurr);
    int pageNo = getCurrPage(hm->perThreadMeta[threadId].OpCurr);
    if ((innerOffset + sizeof(OpLogEntry)) > getPageSize){
        getNextOpPageForThread(hm);
        goto retry;
    }
#ifdef DO_OpLog
    ts = value->timestamp;
    value->timestamp = 0;
#endif
    OpLogEntry* opLogEntry = (OpLogEntry*)
            (hm->operationLog->opBlocks[getOpBlockNo(pageNo)].data
            + getOpPageOff(pageNo) + innerOffset);
    opLogEntry->value = *value;
    hm->perThreadMeta[threadId].OpCurr += sizeof(OpLogEntry);
    if(address > hm->perThreadMeta[threadId].OpCurrMaxAddress){
        hm->perThreadMeta[threadId].OpCurrMaxAddress = address;
    }
    flush_to_nvm((void *)opLogEntry, sizeof(OpLogEntry));
    smp_mb();
#ifdef DO_OpLog
    opLogEntry->value.timestamp = ts;
    flush_to_nvm((void *)(&opLogEntry->value.timestamp), sizeof(uint64_t));
    smp_mb();
#endif
}

inline uint64_t get_hash_key(uint64_t key)
{

    key += ~(key << 32);
    key ^= (key >> 22);
    key += ~(key << 13);
    key ^= (key >> 8);
    key += (key << 3);
    key ^= (key >> 15);
    key += ~(key << 27);
    key ^= (key >> 31);
    if (key > MAX_BUCKET)
        key %= MAX_BUCKET;
    return key;
}

void set_value(HybridMemory* hm, LogAddress &oldAddress, KeyType key, value_t* newValue){
    if (recovery_nvm_block){
        oldAddress = (LogAddress) (newValue);
    }
#ifdef DO_OpLog
    uint64_t bucket = 0;
    newValue->key = key;
    if(hm->use_lock){
        bucket = get_hash_key(key);
        pthread_spin_lock(&hm->spinlocks[bucket]);
    }
#endif
    char* dramAddress;
    checkReadOnlyOffset(hm, OP_WRITE);
#ifndef globalReadOnly
    if((checkInvalidAddress(oldAddress) || getCacheFlag(oldAddress)
        ||(((uint32_t)(oldAddress >> PageSizeBits))<= (hm->perThreadMeta[threadId].readOnlyPages)))){
#else
    if((checkInvalidAddress(newAddress) || getCacheFlag(newAddress) ||(newAddress <= (hm->readOnlyOffset.load())))){
#endif
#ifdef INVALID
        if(!checkInvalidAddress(oldAddress) && getCacheFlag(oldAddress)){
            if((((CacheEntry*)(getActualAddress(oldAddress)))->addressPtr) != &oldAddress){
                vmlog(WARN, "get cache error: oldAddress:%lx :(%lx), ptr:%lx, addreessPtr:%lx, logAddress:%ld, "
                            "entry:%lx,  addreessPtr:%lx, logAddress:%ld"
                            "entry:%lx,  addreessPtr:%lx, logAddress:%ld",
                      oldAddress, (oldAddress - (uint64_t)hm->dataSpace) / (1 << PageSizeBits) * (1 << PageSizeBits) + (uint64_t)hm->dataSpace,
                      &oldAddress, (((CacheEntry*)(getActualAddress(oldAddress)))->addressPtr),
                      (((CacheEntry*)(getActualAddress(oldAddress)))->logAddress),
                      oldAddress+sizeof (CacheEntry),
                      ((((CacheEntry*)(getActualAddress(oldAddress))) + 1)->addressPtr),
                      ((((CacheEntry*)(getActualAddress(oldAddress))) + 1)->logAddress),
                      oldAddress -sizeof (CacheEntry),
                      ((((CacheEntry*)(getActualAddress(oldAddress))) - 1)->addressPtr),
                      ((((CacheEntry*)(getActualAddress(oldAddress))) - 1)->logAddress));
//                SegFault
            }
            invalidAddress((((CacheEntry*)(getActualAddress(oldAddress)))->logAddress), hm);
        } else if(!checkInvalidAddress(oldAddress)){
#ifndef testGetNextAddress
            invalidAddress(oldAddress, hm);
#endif
        }
#endif
        newAddress ++;
getNewAddress:
        uint64_t innerOffset = getCurrInnerOffset(hm->perThreadMeta[threadId].writeCurr);
        uint64_t currPage = (getCurrPage(hm->perThreadMeta[threadId].writeCurr));
#ifdef __DEBUG
        vmlog(DEBUG, "set_value: curr:%ld currPage:%ld, innerOffset:%ld, dramAddress:%ld",
              hm->perThreadMeta[threadId].writeCurr, currPage,innerOffset, dramAddress);
#endif
        hm->perThreadMeta[threadId].writeCurr += memoryUint;
#ifndef testGetNextAddress
        dramAddress = ((innerOffset + memoryUint) <= getPageSize) ?
                (getPageAddress(hm->dataSpace,hm->dramWriteRegionMeta->pages[currPage % PageArraySize])
                + innerOffset)
                : NULL;
        if((innerOffset + memoryUint >= getPageSize) && (innerOffset < getPageSize)){
            getNextWritePageForThread(hm);
        }
        if(dramAddress == NULL){
            checkReadOnlyOffset(hm, OP_WRITE);
            goto getNewAddress;
        }

#endif
        LogAddress newAddress = (currPage << PageSizeBits) + innerOffset;

        while (oldAddress != newAddress){
            oldAddress = newAddress;
        }
//        vmlog(LOG, "set_value: key:%ld, address: %ld", key, newAddress);

//        while (!__sync_bool_compare_and_swap(&oldAddress, oldAddress, newAddress)){
//
//        }
        hm->perThreadMeta[threadId].writeCount ++;
    } else{
        dramAddress =  getPageAddress(hm->dataSpace, hm->dramWriteRegionMeta->pages[getPageIndex(oldAddress, hm)])
                + getInnerOffset(oldAddress, PageOffsetMask);
    }
#ifndef testGetNextAddress
    memcpy(dramAddress, newValue, memoryUint);
#ifdef DO_OpLog
        uint64_t ts = timestamp;
        newValue->timestamp = ts;
    if(hm->use_lock){
        pthread_spin_unlock(&hm->spinlocks[bucket]);
    }
#endif
    if(unlikely((oldAddress >> PageSizeBits) < hm->perThreadMeta[threadId].readOnlyPages)) {
        set_value(hm, oldAddress, key, newValue);
    }
#endif
#ifdef DO_OpLog
        addOperationLog(hm, oldAddress, newValue, ts);
#endif
}

inline char* getDRAMAddress(LogAddress address, HybridMemory* hm){
#ifdef STATICS
    dram_hit[threadId] ++;
#endif
    int pageNo = hm->dramWriteRegionMeta->pages[getPageIndex(address, hm)];
    return getPageAddress(hm->dataSpace, pageNo) + getInnerOffset(address, PageOffsetMask);
}

inline char* getAddressFromNVMBlock(const NVMBlockMeta* nvmBlockMeta, LogAddress logAddress){
    char* pointer = NULL;
#ifdef __DEBUG
    vmlog(DEBUG, "getAddressFromNVMBlock: address:%ld", logAddress);
#endif
    do{
        HashMap* hashMap = (HashMap*) getActualDataAddress(nvmBlockMeta->data);
        assert(nvmBlockMeta->size == (hashMap->num * sizeof (HashBucket) + sizeof (HashMap)));
        pointer = compressHashMapGet(hashMap->m_bucket, hashMap->num, logAddress);
    } while (unlikely(pointer == NULL));
    return pointer;
}

inline char* getNVMAddress(LogAddress address, HybridMemory* hm){
    const NVMBlockMeta* nvmBlockMeta = hm->nvmMeta->blocks
            + getIndex(address, BlockSizeBits);
    if(getCompressFlag(nvmBlockMeta->data)){
        return getAddressFromNVMBlock(nvmBlockMeta, address);
    } else{
        char* pointer = getActualDataAddress(nvmBlockMeta->data)
                + getInnerOffset(address, BlockOffsetMask);
#ifdef __DEBUG
        ((value_t*) pointer)->value[1] = '0';
#endif
        return pointer;

    }
//    value_t *valueAddress = (value_t*)(nvmMeta->blocks[blockIndex].data + innerBlockOffset);
//    for (int  i = 0; i < 1 &&  (innerBlockOffset + i *  memoryUint) < (1 << BlockSizeBits); ++i) {
//        vmlog(LOG,"getNVMAddress address(%ld:(%d,%d)):%ld\n", address, blockIndex, innerBlockOffset + i, *(valueAddress + i));
//    }
}

inline char* getNVMAddressForRead(LogAddress &address, HybridMemory* hm){
    LogAddress localAddr = address;
    if( ((localAddr >> PageSizeBits) >= (uint64_t)(hm->globalDRAMPages))){
        return NULL;
    }
    char* pointer = getNVMAddress(localAddr, hm);
    int readCount = hm->perThreadMeta[threadId].readCount++;
#ifdef USE_Cache
    if((readCount) % CacheSampleFreq == 0 && (!hm->is_writting)){
        uint64_t cacheOffset = getCurrInnerOffset(hm->perThreadMeta[threadId].cacheCurr);
        hm->perThreadMeta[threadId].cacheCurr += sizeof(CacheEntry);
        char* cache = (((cacheOffset + sizeof(CacheEntry)) <= getPageSize)
                && (hm->perThreadMeta[threadId].cachePage != NULL)) ?
                (hm->perThreadMeta[threadId].cachePage + cacheOffset)
                : NULL;
        if(((cacheOffset + sizeof(CacheEntry) >= getPageSize) && (cacheOffset < getPageSize))){
            getNextCachePageForThread(hm);
        }
        if(unlikely(cache == NULL)){
            return pointer;
        }
        *((CacheEntry*)cache) = {localAddr, &address, *((value_t*) pointer)};
        vmlog(LOG, "add cache:%lx, (%lx, %ld)",
              (uint64_t)cache + 1, ((CacheEntry*)cache)->addressPtr, ((CacheEntry*)cache)->logAddress);
        if(address == localAddr){
            address = setCacheFlag(cache);
            return (char *)(&(((CacheEntry*)cache)->value));
        } else{
            *((CacheEntry*)cache) = {0, NULL};
            return pointer;
        }
    } else{
#ifdef __DEBUG
         ((value_t*) pointer)->value[1] = '0';
#endif
        return pointer;
    }
#else
#ifdef __DEBUG

    ((value_t*) pointer)->value[1] = '0';
#endif
    return pointer;
#endif
}

value_t *get_value(HybridMemory* hm, LogAddress addr){
//    printf("get_value: addr: %ld:dramOffset:%ld\n", addr, hm->dramOffset);
    checkReadOnlyOffset(hm, OP_READ);
    return (value_t*) (((addr >> PageSizeBits) >= (uint64_t)(hm->globalDRAMPages))?
            getDRAMAddress(addr, hm) : getNVMAddress(addr, hm));
}
value_t *get_value_for_read(HybridMemory* hm, LogAddress &addr){
    checkReadOnlyOffset(hm, OP_READ);
    LogAddress localAddr = addr;
//
    if(localAddr == AddressNil){
        vmlog(WARN, "localAddr AddressNil");
        return NULL;
    }


    char* address = getCacheFlag(localAddr)? getActualAddress(localAddr) : NULL;
    if(address!= NULL){
        #ifdef __DEBUG
        vmlog(DEBUG, "get_value_for_read: address:%ld, addr:%ld", address, addr);
        ((value_t *)(&(((CacheEntry*)address)->value)))->value[1]='0';
        #endif
#ifdef STATICS

        dram_hit[threadId] ++;
#endif
        return (value_t *)(&(((CacheEntry*)address)->value));
    }
    if ((address = ((localAddr >> PageSizeBits) >= (uint64_t)(hm->globalDRAMPages))?
                    getDRAMAddress(localAddr, hm) : getNVMAddressForRead(addr, hm)) == NULL ){
        return get_value_for_read(hm, addr);
    }
#ifdef   CHECHRes
    char value[value_size];
    memcpy(value, (void *)(((value_t*) address)->value), value_size);
#endif
#ifdef __DEBUG
    ((value_t*) address)->value[1] = '0';
#endif
    return (value_t*) address;
}


inline void getNextBlock(NVMMeta* nvmMeta){
    ++nvmMeta->blockCount;
    nvmMeta->blocks[nvmMeta->blockCount].blockNo = nvmMeta->blockCount;
    if(nvmMeta->blocks[nvmMeta->blockCount].data == NULL){
        getNVMFileName(nvmMeta->blocks[nvmMeta->blockCount].fileName, nvmMeta->blockCount);
        nvmMeta->blocks[nvmMeta->blockCount].data =
                mallocNVM(nvmMeta->blocks[nvmMeta->blockCount].fileName, getBlockSize);
#ifdef DO_OpLog
        flush_to_nvm(&nvmMeta->blocks[nvmMeta->blockCount], sizeof (NVMBlockMeta));
                smp_mb();

#endif
    }
#ifdef DO_OpLog
    flush_to_nvm(nvmMeta, L1_CACHE_BYTES);
            smp_mb();

#endif
}

inline void persistPage(HybridMemory* hm, int pageIndex, char* dest, int pageNum){
//    vmlog(LOG, "pageIndex:%d, pageNum:%d", pageIndex, pageNum);
    DRAMWriteRegionMeta* dramWMeta = hm->dramWriteRegionMeta;
    for (int i = 0; i < pageNum; ++i) {
        int persistPageNo =  dramWMeta->pages[pageIndex];
        char* src = getPageAddress(hm->dataSpace, persistPageNo);
//        int wait;
//        do {
//            int j = 0;
//            for (; (j < MaxThreadNum) && !getPageFlag(pageIndex, dramWMeta, j); ++j) {
//            }
//            if(j >= MaxThreadNum){
//                break;
//            }
//            wait ++;
//            if (wait%1000000 == 0){
//                vmlog(WARN, "wait for pageNo:%d, %d, %x",persistPageNo, i, dramWMeta->pageFlag[i][pageIndex >> 3]);
//            }
//        } while (true);
//        uint64_t v2 = (1024 << PageSizeBits);
//        uint64_t v3 = preparePersisThreshold;
//        vmlog(LOG, "persist pageNo:%d, dramOffset:%ld, readOnlyOffset:%ld, nextAddress:%ld, dest:%lx, src:%lx, %ld, %ld, %ld, %ld",
//              persistPageNo, hm->dramOffset, hm->readOnlyOffset, hm->nextAddress, dest, src,
//               (getValue(hm->nextAddress) - getValue(hm->readOnlyOffset)),v2,v3,((hm->dramWriteRegionMeta->pageNum << PageSizeBits) - preparePersisThreshold));
        nt_store(dest, src, getPageSize);
//        memcpy(dest, src, getPageSize(dramWMeta));
        pageIndex = ( (pageIndex + 1) % PageArraySize);
        vmlog(DEBUG, "persistPage:%lx", src);
        dest += getPageSize;
    }
}
inline void persist_data_in_Block(HybridMemory* hm){
    NVMMeta* nvmMeta = hm->nvmMeta;
    int persistPageIndex = hm->safeForFreePages % PageArraySize;
#ifdef globalReadOnly
    hm->readOnlyOffset.fetch_add(persistLen);
#endif
//    hm->readOnlyOffset += persistLen;
//    __sync_fetch_and_add(&hm->readOnlyOffset, persistLen);
    int blockNo = nvmMeta->blockCount;
    char* blockData = nvmMeta->blocks[blockNo].data;
    int threadNum = hm->persistThreadNum + 1;
    int perThreadPageNum = (persistPageNum + threadNum - 1)/ threadNum;

    for (int i = 0; i < threadNum; i++) {
#ifdef MULTI_WRITE
        volatile NVMWriteAttribute * attribute =  hm->nvmWriteAttributes + i;
        attribute->pageIndex = persistPageIndex;
        attribute->pageNum = (i == (threadNum - 1))? (persistPageNum - i * perThreadPageNum) : perThreadPageNum;
        attribute->dest = (blockData + i * perThreadPageNum * getPageSize);
        attribute->needWrite = true;
#else
        persistPage(hm, persistPageIndex,
                    blockData + i * perThreadPageNum * getPageSize,
                    (i == (threadNum - 1))? (persistPageNum - i * perThreadPageNum) : perThreadPageNum);
#endif
        persistPageIndex = (persistPageIndex + perThreadPageNum) % PageArraySize;
#ifdef MULTI_WRITE
        if(i < threadNum - 1){
            ((NVMWriteAttribute *)attribute)->conditionVariable.notify_one();
        }
#endif
    }
#ifdef MULTI_WRITE
    volatile NVMWriteAttribute * attribute =  hm->nvmWriteAttributes + (threadNum - 1);
    persistPage( hm, attribute->pageIndex, attribute->dest, attribute->pageNum);
    //wait for all thread end
    for (int i = 0; i < threadNum - 1; ++i) {
        while (hm->nvmWriteAttributes[i].needWrite){
        }
    }
#endif
//    __sync_fetch_and_add(&hm->safeForFreeOffset, persistLen);
    getNextBlock(nvmMeta);
    hm->safeForFreePages+= getBlockSize / getPageSize;
    vmlog(WARN, "persist block:%d", blockNo);
}

void NVMWriteThread(NVMWriteAttribute* attribute){
    HybridMemory* hm= attribute->hm;
    std::unique_lock<std::mutex> lock(attribute->m);
    while (!hm->workEnd){
        attribute->conditionVariable.wait(lock, [&attribute, &hm]{return attribute->needWrite || (hm->workEnd);});
        if(hm->workEnd){
            return;
        }
        persistPage( hm, attribute->pageIndex, attribute->dest, attribute->pageNum);
        attribute->needWrite= false;
    }
}


inline void free_page(HybridMemory* hm){
    DRAMWriteRegionMeta* dramWMeta = hm->dramWriteRegionMeta;
    if ((hm->globalDRAMPages) >= (hm->safeForFreePages)){
        return;
    }
    int pageIndex = hm->globalDRAMPages % PageArraySize;
    hm->globalDRAMPages += 1;
    if(dramWMeta->usedPages > dramWMeta->pageNum){
        hm->freePages.Produce(dramWMeta->pages[pageIndex]);
        dramWMeta->usedPages --;
        vmlog(DEBUG, "write usedPages--:%d", dramWMeta->usedPages);
    } else{
        dramWMeta->pages[dramWMeta->allocatedPages % PageArraySize]
            =  (dramWMeta->pages)[pageIndex];
        dramWMeta->allocatedPages ++;
//        vmlog(WARN, "allocatedPages:%d, pageNo:%d", dramWMeta->allocatedPages - 1, dramWMeta->pages[pageIndex]);
    }
}


void persist_thread(HybridMemory *hm){
        int tryCount = 0;

    while (hm->workEnd == false){
#ifndef globalReadOnly
        int satisfyThread = 0;
        int unSatisfy = 0xFFFF;
        for ( int i = hm->clientThreadMaxNum - 1; (i >=0); i--) {
            if(hm->clientLive[i].load() && (hm->perThreadMeta[i].readOnlyPages >= ((hm->safeForFreePages ) + getBlockSize / getPageSize))){
                satisfyThread++;
            } else if(hm->clientLive[i].load() && (tryCount > 1)){
                hm->perThreadMeta[i].readOnlyPages = ((hm->safeForFreePages ) + getBlockSize / getPageSize);
                vmlog(LOG, "persist_thread: thread:%d, new readOnly:%d", i, hm->perThreadMeta[i].readOnlyPages);
                unSatisfy = i;
            } else{
                unSatisfy = i;
            }
        }
        int clientNum=hm->clientThreadNum.load();
        if(clientNum > 0 && unSatisfy < MaxThreadNum && hm->dramWriteRegionMeta->writtenPages >= (hm->dramWriteRegionMeta->allocatedPages - 1)){
            vmlog(WARN, "persist_thread: written pages:%ld, globalDRAM:%d, "
                       "safeForFree:%d, "
                       "usedPages:%d, pageNum:%d, allocatedPages:%d, satisfyThread:%d, unSatisfy:%d, clinetNum:%d, "
                       "pagesBeforeReadOnly:%ld, readOnly:%d",
                       hm->dramWriteRegionMeta->writtenPages.load(), hm->globalDRAMPages,
                       hm->safeForFreePages, hm->dramWriteRegionMeta->usedPages,
                       hm->dramWriteRegionMeta->pageNum, hm->dramWriteRegionMeta->allocatedPages, satisfyThread, unSatisfy, clientNum,
                        hm->pagesBeforeReadOnly,
                    hm->perThreadMeta[unSatisfy].readOnlyPages);

        }
        if((clientNum >0) && (satisfyThread >= clientNum)){
            vmlog(LOG, "persist_thread: written pages:%ld, globalDRAM:%d, "
                       "safeForFree:%d, "
                       "usedPages:%d, pageNum:%d, allocatedPages:%d, satisfyThread:%d, unSatisfy:%d, clinetNum:%d",
                       hm->dramWriteRegionMeta->writtenPages.load(), hm->globalDRAMPages,
                       hm->safeForFreePages, hm->dramWriteRegionMeta->usedPages,
                       hm->dramWriteRegionMeta->pageNum, hm->dramWriteRegionMeta->allocatedPages, satisfyThread, unSatisfy, clientNum);
#else
            if((hm->nextAddress.load() - hm->readOnlyOffset.load()) >= (hm->intervalBeforeReadOnly)){
#endif
            persist_data_in_Block(hm);
                tryCount = 0;
        } else{
            for (int i = 0; i < 10; ++i) {
                if(hm->nvmMeta->blocks[hm->nvmMeta->blockCount + i].data == NULL){
                    getNVMFileName(hm->nvmMeta->blocks[hm->nvmMeta->blockCount + i].fileName, hm->nvmMeta->blockCount + i);
                    hm->nvmMeta->blocks[hm->nvmMeta->blockCount + i].data =
                            mallocNVM(hm->nvmMeta->blocks[hm->nvmMeta->blockCount + i].fileName, getBlockSize);
#ifdef DO_OpLog
                    flush_to_nvm(&hm->nvmMeta->blocks[hm->nvmMeta->blockCount], sizeof (NVMBlockMeta));
                            smp_mb();

#endif
                }
            }
            usleep(usleepTime);
            if(clientNum >= 2 && satisfyThread > 0){
                tryCount ++;
            } else{
                tryCount = 0;
            }
        }

    }
    vmlog(LOG, "persist_thread end");
}


void free_thread(HybridMemory* hm){
#ifdef USE_Cache
    int count = 0;
    uint64_t lastWrite = 0;
    uint64_t lastRead = 0;
    DRAMWriteRegionMeta* dramWMeta = hm->dramWriteRegionMeta;
#endif
    while (hm->workEnd == false){
        bool free = false;
#ifndef globalReadOnly
        if(hm->safeForFreePages > ((hm->globalDRAMPages))){
            vmlog(DEBUG, "free_thread: written pages:%ld, globalDRAM:%d, "
                       "readOnly:%ld, safeForFree:%d, "
                       "usedPages:%d, pageNum:%d, allocatedPages:%d",
                       hm->dramWriteRegionMeta->writtenPages.load(), hm->globalDRAMPages, hm->perThreadMeta[0].readOnlyPages,
                       hm->safeForFreePages, hm->dramWriteRegionMeta->usedPages,
                       hm->dramWriteRegionMeta->pageNum, hm->dramWriteRegionMeta->allocatedPages);
            free_page(hm);
            free = true;
        }
#ifdef USE_Cache

        DRAMWriteRegionMeta * dramWriteRegionMeta = hm->dramWriteRegionMeta;

        while ((!hm->is_writting)
        && (dramWriteRegionMeta->usedPages > dramWriteRegionMeta->pageNum)
        && (dramWriteRegionMeta->allocatedPages > (dramWriteRegionMeta->writtenPages + MaxThreadNum))){
            int dropPageIndex = (dramWriteRegionMeta->allocatedPages - 1) % PageArraySize;
            hm->freePages.Produce((dramWriteRegionMeta->pages)[dropPageIndex]);
            dramWriteRegionMeta->allocatedPages --;
            dramWriteRegionMeta->usedPages --;
            free = true;
        }
        while((!hm->is_writting) && (hm->freePages.head < hm->freePages.tail + 10) && (hm->pagesBeforeReadOnly > 0)){
            hm->pagesBeforeReadOnly --;
        }
        if(!hm->is_writting && free){
            vmlog(WARN, "write usedPages:%d, pageNum:%d, writtenPage:%d, allocate:%d",
                  dramWriteRegionMeta->usedPages, dramWriteRegionMeta->pageNum,
                  dramWriteRegionMeta->writtenPages.load(), dramWriteRegionMeta->allocatedPages);
        }
#endif
#else
        if(hm->readOnlyOffset.load() >= (hm->globalDRAMOffset + getPageSize + (pagesAfterReadOnly << PageSizeBits))){
            free_page(hm);
        }
#endif
#ifdef USE_Cache
        count ++;
        if(count%checkFreq == 0){
            uint64_t newWrite = 0;
            uint64_t newRead = 0;
            for ( int i = hm->clientThreadMaxNum - 1; (i >=0); i--) {
                newWrite += hm->perThreadMeta[i].writeCount;
                newRead += hm->perThreadMeta[i].readCount;
            }
            uint64_t write = newWrite - lastWrite;
            uint64_t  read = newRead - lastRead;
            hm->is_writting = (write > 0) || (read == 0);
            if (read){
                double writeRatio = (write > 0)? 1.0 : ((write * deltaW) / (double)(write * deltaW + read * deltaR));
                int oldPageNum = dramWMeta->pageNum;
                dramWMeta->pageNum =
                        (((TOTAL_PAGE_NUM * writeRatio)) > (pagesAfterReadOnly + 1) )?
                        (TOTAL_PAGE_NUM * writeRatio) : (pagesAfterReadOnly + 1);
                hm->cacheRegionMeta->pageNum = TOTAL_PAGE_NUM - dramWMeta->pageNum;
                lastRead = newRead;
                lastWrite = newWrite;
                if(oldPageNum!=dramWMeta->pageNum){
                    vmlog(DEBUG, "change pageNum: write:%d, cache:%d",
                          dramWMeta->pageNum, hm->cacheRegionMeta->pageNum);
                }
            }
        }
        while ((dramWMeta->usedPages < dramWMeta->pageNum) && !hm->workEnd){
            int pageNo;
            if(hm->freePages.Consume(pageNo)){
                dramWMeta->pages[dramWMeta->allocatedPages % PageArraySize]
                    =  pageNo;
                dramWMeta->allocatedPages ++;
                vmlog(DEBUG, "write allocatedPages:%d, pageNo:%d：%lx", dramWMeta->allocatedPages - 1, pageNo,
                      getPageAddress(hm->dataSpace, pageNo));
                dramWMeta->usedPages ++;
                vmlog(DEBUG, "write usedPages++:%d", dramWMeta->usedPages);
                hm->pagesBeforeReadOnly =
                        (((uint64_t)dramWMeta->usedPages) > preparePersisPages) ?
                        (((uint64_t)dramWMeta->usedPages ) - preparePersisPages) :
                        getPageSize;
                vmlog(DEBUG, "write add page:%d", pageNo);
            } else{
                vmlog(DEBUG, "write add no page");
                break;
            }
        }
#endif
        if(!free){
            usleep(usleepTime);
        }
    }
    vmlog(LOG, "free_thread end");
}

void free_op_thread(HybridMemory* hm){
    OperationLog * operationLog = hm->operationLog;

    while (hm->workEnd == false){
        bool free = false;
        for (int i = operationLog->miniOpBlock; i < operationLog->writtenPages / (getOpBlockSize/getPageSize); ++i) {
            int blockNo = i % MaxOpBlockNum;
            int satisfyThread = 0;
            for (int j = 0; j < hm->clientThreadMaxNum; ++j) {
                if(hm->clientLive[i].load() && getCurrPage(hm->perThreadMeta[i].OpCurr) > i * ((getOpBlockSize/getPageSize))){
                    satisfyThread++;
                }
            }
            int clientNum=hm->clientThreadNum.load();
            if((clientNum <=0) || (satisfyThread < clientNum)){
                break;
            }
            if(operationLog->opBlocks[blockNo].maxAddress <= (((uint64_t)hm->safeForFreePages) * getPageSize)){
                free = true;
                operationLog->allocatedPages += getOpBlockSize / getPageSize;
                operationLog->miniOpBlock = i + 1;
                vmlog(WARN, "free_op_thread free:%d", i);
            } else{
//                vmlog(WARN, "free_op_thread  block:%d, miniOpBlock:%d, max:%ld, safeForFreePages:%ld",
//                      blockNo, operationLog->miniOpBlock, operationLog->opBlocks[blockNo].maxAddress.load() ,
//                      ((uint64_t)hm->safeForFreePages) * getPageSize);
            }
        }
        flush_to_nvm((void *)hm->operationLog, 2 * sizeof(uint64_t));
        smp_mb();

        if(!free){
            usleep(500);
        }
    }

}

void invalid_address_thread(HybridMemory* hm){
    while (hm->workEnd == false){
        int satisfyThread = 0;
        for ( int i = hm->clientThreadMaxNum - 1; (i >=0); i--) {
            if(hm->clientLive[i] && getCurrPage(hm->perThreadMeta[i].invalidCurr) >=
                ((uint64_t)(hm->invalidAddressArray->persistedIndex >> PageSizeBits ) + InvalidAddressCachePages)){
                satisfyThread++;
            }
        }
        int clientNum=hm->clientThreadNum.load();
        if((clientNum >0) && (satisfyThread >= clientNum)){
            vmlog(DEBUG, "invalid_address_thread: written pages:%ld,  allocatedPages:%d",
                  hm->invalidAddressArray->writtenPages.load(),  hm->invalidAddressArray->allocatedPages);
            persist_invalid_address(hm->invalidAddressArray);
        } else{
            usleep(usleepTime);
        }
    }
    vmlog(LOG, "invalid_address_thread end");
}

inline bool dropCachePage(HybridMemory* hm){
    CacheRegionMeta* cacheRegionMeta = hm->cacheRegionMeta;
    if(cacheRegionMeta->allocatedPages <= cacheRegionMeta->dropPages){
        return false;
    }
    int satisfyThread = 0;
    for ( int i = hm->clientThreadMaxNum - 1; (i >=0); i--) {
        if(hm->clientLive[i] &&
        (getCurrPage(hm->perThreadMeta[i].cacheCurr) >= ((uint64_t)cacheRegionMeta->dropPages + 1))){
            satisfyThread++;
        }
    }
    if(hm->clientThreadNum <= 0 || (satisfyThread < hm->clientThreadNum.load())){
        vmlog(DEBUG, "dropCachePage: dropPages:%d, satisfy threadNum:%d",
              cacheRegionMeta->dropPages, satisfyThread);
        return false;
    }
    int dropPageIndex = cacheRegionMeta->dropPages % PageArraySize;
    char* cache = getPageAddress(hm->dataSpace, cacheRegionMeta->pages[dropPageIndex]);
    int reset_fault = 0;
    for (int i = 0; (i + sizeof (CacheEntry)) < getPageSize; i += sizeof(CacheEntry)) {
        CacheEntry* cacheEntry = (CacheEntry*)(cache + i);
        LogAddress ptr = (LogAddress)cacheEntry;
        ptr = setCacheFlag(ptr);
        vmlog(DEBUG, "reset: ptr:%ld, LogAddress:%ld", ptr, cacheEntry->logAddress);
        if(cacheEntry->addressPtr == NULL){
            break;
        }
        if(!__sync_bool_compare_and_swap(cacheEntry->addressPtr, ptr, cacheEntry->logAddress)){
            reset_fault ++;
        } else{
            vmlog(DEBUG, "reset: ptr:%ld, LogAddress:%ld", ptr, cacheEntry->logAddress);
        }
    }
//    vmlog(WARN, "reset fault:%d", reset_fault);
    vmlog(DEBUG, "dropCachePage: drop cache page:%d", cacheRegionMeta->allocatedPages);
    cacheRegionMeta->dropPages ++;
    vmlog(WARN, "drop page:%p, dropPages:%d, usedPages:%d, pageNum:%d",
          (void *)cache,  cacheRegionMeta->dropPages, cacheRegionMeta->usedPages, cacheRegionMeta->pageNum);
    if(cacheRegionMeta->usedPages > cacheRegionMeta->pageNum){
        hm->freePages.Produce((cacheRegionMeta->pages)[dropPageIndex]);
        cacheRegionMeta->usedPages --;
    } else{
        cacheRegionMeta->pages[cacheRegionMeta->allocatedPages % PageArraySize]
            =  (cacheRegionMeta->pages)[dropPageIndex];
        vmlog(DEBUG, "cache allocatedPages:%d:%lx", cacheRegionMeta->allocatedPages,  cache);
        cacheRegionMeta->allocatedPages ++;
    }
    return true;

}

void read_cache_thread(HybridMemory* hm){
    CacheRegionMeta* cacheRegionMeta = hm->cacheRegionMeta;
    while (hm->workEnd == false){
        bool free = false;
        if(((cacheRegionMeta->writtenPages) > (cacheRegionMeta->allocatedPages - CacheFreePageThreshold))
            || (cacheRegionMeta->usedPages > cacheRegionMeta->pageNum) ){
            if(cacheRegionMeta->usedPages < cacheRegionMeta->pageNum){
                int pageNo;
                if(hm->freePages.Consume(pageNo)){
                    cacheRegionMeta->pages[cacheRegionMeta->allocatedPages % PageArraySize]
                    =  pageNo;
                    vmlog(LOG, "cache allocatedPages:%d:%lx", cacheRegionMeta->allocatedPages, getPageAddress(hm->dataSpace, pageNo));
                    cacheRegionMeta->allocatedPages ++;
                    cacheRegionMeta->usedPages ++;
                } else{
                    free = dropCachePage(hm);
                }
            } else{
                free = dropCachePage(hm);
            }

//            while (cacheRegionMeta->usedPages > cacheRegionMeta->pageNum && ((cacheRegionMeta->allocatedPages-cacheRegionMeta->writtenPages) > MaxThreadNum)){
            while (hm->is_writting && (cacheRegionMeta->allocatedPages>cacheRegionMeta->writtenPages)){
                int dropPageIndex = (cacheRegionMeta->allocatedPages - 1) % PageArraySize;
                hm->freePages.Produce((cacheRegionMeta->pages)[dropPageIndex]);
                cacheRegionMeta->allocatedPages --;
                cacheRegionMeta->usedPages --;
                free = true;
            }

            if(!free){
                usleep(usleepTime);
            } else{
                vmlog(WARN, "cache usedPages:%d", cacheRegionMeta->usedPages);
            }

        } else{
            usleep(500);
        }
    }
    vmlog(DEBUG, "read_cache_thread end");
}

value_t hm_delete(HybridMemory* hm, key_t key, fn_delete_t* func){
    LogAddress address = (LogAddress)func(key);
    value_t* valuePtr = get_value(hm, address);
    value_t value = *valuePtr;
    invalidAddress(address, hm);
    return value;
}

void hm_timer(HybridMemory* hm) {
    while (!hm->workEnd) {
        usleep(10);
        timestamp++;
    }
}

inline bool recovery_insert(HybridMemory* hm, fn_recovery_insert_t *recoveryInsert, void * index, KeyType key, LogAddress address, uint64_t timestamp){
    int bucket = get_hash_key(key);
    bool insert = false;
    pthread_spin_lock(&hm->spinlocks[bucket]);
    TempHashBucket* tempHashBucket = hm->tempHashtable + bucket;
    bool  found = false;
    while(!found){
        for (int l = 0; l < tempHashBucket->num; ++l) {
            if(tempHashBucket->key[l] == key){
                if(tempHashBucket->timestamp[l] < timestamp){
//                    vmlog(WARN, "recovery_insert invalid key:%ld, address:%ld", key, tempHashBucket->address[l]);
                    invalidAddress(tempHashBucket->address[l], hm);
                    recoveryInsert(index, key, address);
                    tempHashBucket->timestamp[l] = (timestamp);
                    tempHashBucket->address[l] = (address);
                    insert = true;
                } else if(tempHashBucket->timestamp[l] > timestamp){
                    invalidAddress(address, hm);
                }
                found = true;
                break;
            }
        }
        if(found){
            break;
        }
        if(tempHashBucket->num < TEMP_ENTRIES_PER_BUCKET){
            recoveryInsert(index, key, address);
            tempHashBucket->timestamp[tempHashBucket->num] = timestamp;
            tempHashBucket->address[tempHashBucket->num] = address;
            tempHashBucket->key[tempHashBucket->num] = key;
            tempHashBucket->num ++;
            found = true;
            insert = true;
            break;
        }
        if(tempHashBucket->next == NULL){
            break;
        }
        tempHashBucket = tempHashBucket->next;
    }
    if(!found){
        TempHashBucket* newbucket = static_cast<TempHashBucket *>(malloc(sizeof(TempHashBucket)));
        newbucket->key[0] = key;
        newbucket->timestamp[0] = timestamp;
        newbucket->address[0] = address;
        newbucket->num = 1;
        newbucket->next = NULL;
        tempHashBucket->next = newbucket;
        recoveryInsert(index, key, address);
        insert = true;
    }
    pthread_spin_unlock(&hm->spinlocks[bucket]);
//    if(insert && (getIndex(address, BlockSizeBits) == 0)){
//        vmlog(WARN, "recovery_insert key:%ld, address:%ld", key, address);
//    }
    return insert;
}
#ifdef DO_OpLog
typedef struct OpLogAttr{
    HybridMemory* hm;
    fn_insert_t* insert;
    fn_recovery_insert_t* recoveryInsert;
    void * index;
    int startIndex;
    int endIndex;
    int threadId;
    }OpLogAttr;
void recoverOpLog(OpLogAttr * opLogAttr){
    HybridMemory* hm = opLogAttr->hm;
    fn_insert_t* insert = opLogAttr->insert;
    fn_recovery_insert_t* recoveryInsert = opLogAttr->recoveryInsert;
    void * index = opLogAttr->index;
    OperationLog * opLog = hm->operationLog;
    threadId = opLogAttr->threadId;
    for (int i = opLogAttr->startIndex; i >= opLogAttr->endIndex; i--) {
        char fileName[FileNameMaxLen];
        getOpBlockNVMFileName(fileName, opLog->opBlocks[i].fileNo);
        opLog->opBlocks[i].data = openNVM(fileName, getOpBlockSize);
        char* data = opLog->opBlocks[i].data;
        vmlog(WARN, "recovery : opblock:%d",i);
        for (int j = 0; j + getPageSize < getOpBlockSize; j += getPageSize) {
//            vmlog(WARN, "recovery : opblock:%d, page:%d",i, j / getPageSize);

            for (int k = 0; k + sizeof(OpLogEntry)< getPageSize; k += sizeof(OpLogEntry)) {
                OpLogEntry *logEntry = (OpLogEntry*) (data + j + k);
                if((logEntry->value.key == AddressNil)){
                    break;
                }
                if(logEntry->value.key == 0){
                    continue;
                }
                int bucket = get_hash_key(logEntry->value.key);
                pthread_spin_lock(&hm->spinlocks[bucket]);
                TempHashBucket* tempHashBucket = hm->tempHashtable + bucket;
                bool  found = false;
                while(!found){
                    for (int l = 0; l < tempHashBucket->num; ++l) {
                        if(tempHashBucket->key[l] == logEntry->value.key){
                            found = true;
                            if(tempHashBucket->timestamp[l] < logEntry->value.timestamp){
                                insert(index, logEntry->value.key, logEntry->value);
                                tempHashBucket->timestamp[l] = logEntry->value.timestamp;
                                tempHashBucket->address[l] = AddressNil;
                            }
                            break;
                        }
                    }
                    if(found){
                        break;
                    }
                    if(tempHashBucket->num < TEMP_ENTRIES_PER_BUCKET){

                        insert(index, logEntry->value.key, logEntry->value);
                        tempHashBucket->timestamp[tempHashBucket->num] = logEntry->value.timestamp;
                        tempHashBucket->key[tempHashBucket->num] = logEntry->value.key;
                        tempHashBucket->address[tempHashBucket->num] = AddressNil;
                        tempHashBucket->num ++;
                        found = true;
                        break;
                    }
                    if(tempHashBucket->next == NULL){
                        break;
                    }
                    tempHashBucket = tempHashBucket->next;
                }
                if(!found){
                    TempHashBucket* newbucket = static_cast<TempHashBucket *>(malloc(sizeof(TempHashBucket)));
                    newbucket->key[0] = logEntry->value.key;
                    newbucket->timestamp[0] = logEntry->value.timestamp;
                    newbucket->address[0] = AddressNil;
                    newbucket->num = 1;
                    tempHashBucket->next = newbucket;
                    newbucket->next = NULL;
                    insert(index, logEntry->value.key, logEntry->value);
                }
                pthread_spin_unlock(&hm->spinlocks[bucket]);
            }

        }

        vmlog(WARN, "recovery : opblock:%d end",i);
    }
    hm->clientEnd();

    }
void recoverNVM(OpLogAttr * opLogAttr){
    HybridMemory* hm = opLogAttr->hm;
    fn_insert_t* insert = opLogAttr->insert;
    fn_recovery_insert_t* recoveryInsert = opLogAttr->recoveryInsert;
    void * index = opLogAttr->index;
    threadId = opLogAttr->threadId;
    NVMMeta* nvmMeta = hm->nvmMeta;
    for (int i = opLogAttr->startIndex; i < opLogAttr->endIndex; ++i) {
        nvmMeta->blocks[i].reorgnaized = 0;
        for (int j = 0; j < MaxThreadNum; ++j) {
            nvmMeta->blocks[i].perThreadMeta[j].deleted = 0;
            nvmMeta->blocks[i].perThreadMeta[j].lastDelete = -2;
            nvmMeta->blocks[i].perThreadMeta[j].oldLast = -2;
            nvmMeta->blocks[i].perThreadMeta[j].lastReorgnaize = -2;
        }
        if(getCompressFlag(nvmMeta->blocks[i].data)){
            nvmMeta->blocks[i].data= openNVM(nvmMeta->blocks[i].fileName, nvmMeta->blocks[i].size);
            HashMap *hashMap = (HashMap*)nvmMeta->blocks[i].data;
            setCompressFlag(nvmMeta->blocks[i].data);
            flush_to_nvm(nvmMeta->blocks + i, sizeof(NVMBlockMeta));
            smp_mb();

            vmlog(WARN, "recovery : block:%d, name:%s, compress num:%d",
                  i, nvmMeta->blocks[i].fileName, hashMap->num);

            for (uint64_t j = 0; j < hashMap->num; ++j) {

                HashBucket* hashBucket = &hashMap->m_bucket[j];
                if(recovery_insert(hm, recoveryInsert, index, hashBucket->value.key, hashBucket->key, hashBucket->value.timestamp)){
                }
            }
        } else{
            nvmMeta->blocks[i].data= openNVM(nvmMeta->blocks[i].fileName, getBlockSize);
            LogAddress startAddress = ((uint64_t)nvmMeta->blocks[i].blockNo) << BlockSizeBits;
            vmlog(WARN, "recovery : block:%d",i);
            for (uint64_t j = 0; j < nvmMeta->blocks[i].size; j+= getPageSize) {
                for (uint64_t k = 0; (k + memoryUint) <= getPageSize ; k += memoryUint) {
                    LogAddress address = j + k;
                    value_t *value = (value_t *) (nvmMeta->blocks[i].data + address);
                    if(value->key == 0){
                        continue;
                    }
                    if(recovery_insert(hm, recoveryInsert, index, value->key, startAddress + address, value->timestamp)){
                    }
                }
            }
        }
        vmlog(WARN, "recovery : block:%d end",i);

    }
    hm->clientEnd();

}
#define RECOVER_THREAD 16
bool hm_recovery(HybridMemory* hm, fn_insert_t* insert, fn_recovery_insert_t* recoveryInsert, void * index){
    vmlog(WARN, "recovery start");
    bool recovery = false;
    uint64_t opLogRecover = 0, dataRecovery = 0, opFound = 0;
    NVMMeta *nvmMeta = hm->nvmMeta;
    OperationLog * opLog = hm->operationLog;
    int init_blockCount = nvmMeta->blockCount;
    if(hm->spinlocks == NULL && (nvmMeta->blockCount!=0 || opLog->writtenPages != 0)){
        hm->spinlocks = static_cast<pthread_spinlock_t *>(malloc(MAX_BUCKET * sizeof(pthread_spinlock_t)));
        for (int i = 0; i < MAX_BUCKET; ++i) {
            pthread_spin_init(hm->spinlocks + i, NULL);
        }
    }


    if(init_blockCount!=0){
        hm->dramWriteRegionMeta->writtenPages.fetch_add(nvmMeta->blockCount * (getBlockSize / getPageSize));
        hm->dramWriteRegionMeta->allocatedPages+= nvmMeta->blockCount * (getBlockSize / getPageSize);
        hm->safeForFreePages = nvmMeta->blockCount * (getBlockSize / getPageSize);
        hm->globalDRAMPages = nvmMeta->blockCount * (getBlockSize / getPageSize);
        vmlog(WARN, "written:%ld, allocated:%ld, globalDRAM:%ld, safeforFree:%ld",
              hm->dramWriteRegionMeta->writtenPages.load(),
              hm->dramWriteRegionMeta->allocatedPages,
              hm->safeForFreePages, hm->globalDRAMPages);
        //get current nvm block
        nvmMeta->blocks[nvmMeta->blockCount].blockNo = nvmMeta->blockCount;
        getNVMFileName(nvmMeta->blocks[nvmMeta->blockCount].fileName, nvmMeta->blockCount);
        nvmMeta->blocks[nvmMeta->blockCount].data =
                mallocNVM(nvmMeta->blocks[nvmMeta->blockCount].fileName, getBlockSize);
        flush_to_nvm(&nvmMeta->blocks[nvmMeta->blockCount], sizeof (NVMBlockMeta));
        smp_mb();
        for (int i = 0; i <= nvmMeta->blockCount; ++i) {
            for (int j = 0; j < MaxThreadNum; ++j) {
                nvmMeta->blocks[i].perThreadMeta[j].init();
            }
        }
        for (int i = nvmMeta->blockCount + 1; i < MaxBlockNum; ++i) {
            nvmMeta->blocks[i].data = NULL;
        }
    }
    hm->tempHashtable = static_cast<TempHashBucket *>(malloc(sizeof(TempHashBucket) * MAX_BUCKET));
    memset(hm->tempHashtable, 0, sizeof(TempHashBucket) * MAX_BUCKET);

    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    threadId = 0;
    hm->initClient(RECOVER_THREAD);
    if(opLog->writtenPages != 0){
        recovery = true;
        vmlog(WARN, "oplog: written:%ld, allocated:%ld, mini:%ld",
              opLog->writtenPages.load(),
              opLog->allocatedPages,
              opLog->miniOpBlock);

        OpLogAttr opLogAttrs[RECOVER_THREAD];
        pthread_t pthreads[RECOVER_THREAD];
        // recover opLog
        int startIndex =  (int )((opLog->writtenPages + getOpBlockSize / getPageSize - 1 ) / (getOpBlockSize / getPageSize)) - 1;
        int endIndex = opLog->miniOpBlock;
        int perThread = (startIndex - endIndex + 1) / RECOVER_THREAD;
        int mod = (startIndex - endIndex + 1) % RECOVER_THREAD;
        for (int i = 0; i < RECOVER_THREAD;  i ++) {
            opLogAttrs[i].hm = hm;
            opLogAttrs[i].insert = insert;
            opLogAttrs[i].recoveryInsert = recoveryInsert;
            opLogAttrs[i].index = index;
            opLogAttrs[i].startIndex =  startIndex - i * perThread;
            opLogAttrs[i].threadId = i;
            if(mod){
                if(mod > i){
                    opLogAttrs[i].startIndex -= i;
                } else{
                    opLogAttrs[i].startIndex -= mod;
                }
            }
            opLogAttrs[i].endIndex = opLogAttrs[i].startIndex - (perThread - 1);
            if(mod && (i <mod)){
                opLogAttrs[i].endIndex--;
            }
            if(opLogAttrs[i].endIndex < endIndex){
                opLogAttrs[i].endIndex = endIndex;
            }
            pthread_create(&pthreads[i], NULL, reinterpret_cast<void *(*)(void *)>(recoverOpLog), opLogAttrs + i);
        }
        for (int i = 0; i < RECOVER_THREAD; ++i) {
            pthread_join(pthreads[i], NULL);
        }
    }

    clock_gettime(CLOCK_REALTIME, &endTmp);
    double usedTime1 = (endTmp.tv_sec - startTmp.tv_sec)*1e3 + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-6;
    clock_gettime(CLOCK_REALTIME, &startTmp);

    hm->initClient(1);
    if(init_blockCount!=0){
        recovery_nvm_block = true;
        recovery = true;
        OpLogAttr opLogAttrs[RECOVER_THREAD];
        pthread_t pthreads[RECOVER_THREAD];
        int perThread = init_blockCount / RECOVER_THREAD;
        int mod = init_blockCount % RECOVER_THREAD;
        vmlog(WARN, "recovery : nvmMeta.count:%d, perThread:%d", init_blockCount, perThread);

        for (int i = 0; i < (RECOVER_THREAD > init_blockCount ? init_blockCount : RECOVER_THREAD);  i ++) {
            opLogAttrs[i].hm = hm;
            opLogAttrs[i].insert = insert;
            opLogAttrs[i].recoveryInsert = recoveryInsert;
            opLogAttrs[i].index = index;
            opLogAttrs[i].startIndex =  i * perThread;
            opLogAttrs[i].threadId = i;
            if(mod){
                if(mod > i){
                    opLogAttrs[i].startIndex += i;
                } else{
                    opLogAttrs[i].startIndex += mod;
                }
            }
            opLogAttrs[i].endIndex = opLogAttrs[i].startIndex + perThread;
            if(mod && (i <mod)){
                opLogAttrs[i].endIndex ++;
            }
            if(opLogAttrs[i].endIndex > init_blockCount){
                opLogAttrs[i].endIndex = init_blockCount;
            }
            pthread_create(&pthreads[i], NULL, reinterpret_cast<void *(*)(void *)>(recoverNVM), opLogAttrs + i);
        }
        for (int i = 0; i < (RECOVER_THREAD > init_blockCount ? init_blockCount : RECOVER_THREAD); ++i) {
            pthread_join(pthreads[i], NULL);
        }

    } else{
        nvmMeta->init();
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    double usedTime2 = (endTmp.tv_sec - startTmp.tv_sec)*1e3 + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-6;

    hm->clientEnd();
    hm->operationLog->init();
    free(hm->tempHashtable);
    hm->tempHashtable = NULL;
    vmlog(WARN, "recovery end: opLogRecover:%ld,  dataRecover:%ld, recovery:%.2fms, %.2fms", opLogRecover, dataRecovery, usedTime1, usedTime2);
    do_recovery = false;
    recoveryTime = usedTime1 + usedTime2;
    return recovery;
}


#endif


static char *g_pop;

std::atomic<uint64_t> allocated(0);

void init_nvmm_pool()
{

    g_pop = mallocNVM(NVMMetaFile, MEM_SIZE);
    if(g_pop ==NULL){
        printf("malloc nvm error \n");
        exit(0);
    }
}

void *nvm_alloc(size_t size)
{
    uint64_t off = allocated.fetch_add(size);
    if(off + size > MEM_SIZE){
        printf(" nvm out of memory \n");
        exit(0);
    }
    return (void*)(g_pop + off);
}