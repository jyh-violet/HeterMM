cmake_minimum_required(VERSION 3.16)
project(hybridMM)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "-O3 -g -march=skylake-avx512 -Wall -faligned-new -rdynamic")
set(CMAKE_BUILD_TYPE Debug)

set(lmn_SRCS "src/index/")
set(index_SRCS "src/index/")


IF (indexType STREQUAL test_clht)
    add_definitions(-Dtest_clht)
ELSEIF(indexType STREQUAL test_lfht)
    add_definitions(-Dtest_lfht)
ELSEIF(indexType STREQUAL test_bltree)
    add_definitions(-Dtest_bltree)
ELSEIF(indexType STREQUAL test_bwtree)
    add_definitions(-Dtest_bwtree)
ELSEIF(indexType STREQUAL test_utree)
    add_definitions(-Dtest_utree)
ENDIF()


OPTION(USE_HMM "OPTION" OFF)
OPTION(MULTI_WRITE "OPTION" OFF)
OPTION(USE_NVM "OPTION" ON)
OPTION(COMPRESS "OPTION" ON)
OPTION(INVALID "OPTION" ON)
OPTION(INVALID_PERSIST "OPTION" ON)
OPTION(USE_Cache "OPTION" OFF)
OPTION(DO_OpLog "OPTION" OFF)

IF (USE_HMM )
   MESSAGE( STATUS "USE_HMM = ${USE_HMM}.")
    add_definitions(-DUSE_HMM)
ENDIF()

IF (MULTI_WRITE )
    MESSAGE( STATUS "MULTI_WRITE = ${MULTI_WRITE}.")
    add_definitions(-DMULTI_WRITE)
ENDIF()

IF ( USE_NVM)
    MESSAGE( STATUS "USE_NVM = ${USE_NVM}.")
    add_definitions(-DUSE_NVM)
ENDIF()

IF ( COMPRESS)
    MESSAGE( STATUS "COMPRESS = ${COMPRESS}.")
    add_definitions(-DCOMPRESS)
ENDIF()

IF ( INVALID )
    MESSAGE( STATUS "INVALID = ${INVALID}.")
    add_definitions(-DINVALID)
ENDIF()

IF ( INVALID_PERSIST )
    MESSAGE( STATUS "INVALID_PERSIST = ${INVALID_PERSIST}.")
    add_definitions(-DINVALID_PERSIST)
ENDIF()

IF (USE_Cache)
    MESSAGE( STATUS "USE_Cache = ${USE_Cache}.")
    add_definitions(-DUSE_Cache)
ENDIF()

IF (DO_OpLog)
    MESSAGE( STATUS "DO_OpLog = ${DO_OpLog}.")
    add_definitions(-DDO_OpLog)
ENDIF()

IF (NVM_ONLY)
    MESSAGE( STATUS "NVM_ONLY = ${NVM_ONLY}.")
    add_definitions(-DNVM_ONLY)
ENDIF()


configure_file("src/config.h.in" "src/config.h")


add_library(lmntal ${lmn_SRCS}/cc_hashmap.cc  ${lmn_SRCS}/cc_hashmap.h ${lmn_SRCS}/chain_hashmap.cc ${lmn_SRCS}/chain_hashmap.h
        ${lmn_SRCS}/hashmap.cc ${lmn_SRCS}/hashmap.h ${lmn_SRCS}/lf_chain_hashmap.cc ${lmn_SRCS}/lf_chain_hashmap.h
        src/index/thread.cc src/index/thread.h)

add_library(index ${index_SRCS}clht_lb_res.h ${index_SRCS}clht_lb_res.cpp  src/index/clht_gc.cpp
        ${index_SRCS}bptree.h ${index_SRCS}bptree.cpp
        ${index_SRCS}bltree.h ${index_SRCS}bltree.cpp
        ${index_SRCS}bwtree.h ${index_SRCS}bwtree.cpp
        ${index_SRCS}utree.h ${index_SRCS}utree.cpp
        ${index_SRCS}atomic_stack.h ${index_SRCS}bloom_filter.h ${index_SRCS}sorted_small_set.h
         src/index/atomic_ops.h
        src/index/ssmem.h src/index/ssmem.c src/index/sspfd.h src/index/utils.h src/index/utree.cpp)

add_library(hm src/hm.cpp src/hm.hpp src/tool.h src/threadPool.h)
#add_executable(test src/test.cpp)
#target_link_libraries(test  hm lmntal -lpmem -lpthread)
add_executable(hybridMM src/main.cpp)
target_link_libraries(hybridMM hm lmntal -lpmem -lpthread  -lconfig -lpmemobj)

add_executable(bench src/bench.cpp)
target_link_libraries(bench hm index lmntal -lpmem -lpthread  -lconfig -lpmemobj)

add_executable(ycsb src/ycsb.cpp)
target_link_libraries(ycsb hm index lmntal -lpmem -lpthread  -lconfig -latomic -lpmemobj)