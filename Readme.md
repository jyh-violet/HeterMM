# Dependencies:
```console
sudo apt-get install libevent-dev
sudo apt-get install libconfig-dev
sudo apt-get install libpmem1 librpmem1 libpmemblk1 libpmemlog1 libpmemobj1 libpmempool1
sudo apt-get install libpmem-dev librpmem-dev libpmemblk-dev libpmemlog-dev libpmemobj-dev libpmempool-dev libpmempool-dev
sudo apt-get install libpmem1-debug librpmem1-debug libpmemblk1-debug libpmemlog1-debug libpmemobj1-debug libpmempool1-debug
```

# Directories
index/: source codes of combined indexes, including clht(clht_lb_res.cc/.h), lfht(lf_chain_hashmap.cc/.h), and b+tree (utree.cpp/.h)
hm.cpp/.h: source code of the framework
ycsb.cpp: source code of benchmarking
# Compile
```console
cmake .
make
```
parameters for cmake:
cmake -Dvalue_size=8 -DindexType=test_lfht
- value_size=8 : bytes of value_size
- indexType=test_lfht : combined index types, candidates: 'test_clht' 'test_lfht' 'test_utree'
- DO_OpLog=ON : enable operation log
- dram_size: size in GBs of dram region of the storage engine

# Genenrate data

```
./workloads/generate_all_workloads.sh 
```

# Config file
config.cfg: which will be read by the program automatically and should be in the same path as the executable file. The following is an example.
```
# recordcount in YCSB config file
LOAD_SIZE = 200000000

# operationcount in YCSB config file
TXN_SIZE = 100000000

# path of the data files generated in the former step
LOAD_FILE = "memcached_client/workloads/data/loadc_zipf_int_200M.dat" 

TXN_FILE = "memcached_client/workloads/data/txnsc_zipf_int_200M.dat"

threadNum= 32
```

# run

```console
./ycsb
```
result can be found in ./log file

# Relative Publication
```
@article{DBLP:journals/fcsc/JiHZ24,
  author       = {Yunhong Ji and
                  Wentao Huang and
                  Xuan Zhou},
  title        = {HeterMM: applying in-DRAM index to heterogeneous memory-based key-value
                  stores},
  journal      = {Frontiers Comput. Sci.},
  volume       = {18},
  number       = {4},
  pages        = {184612},
  year         = {2024},
  url          = {https://doi.org/10.1007/s11704-024-3713-0},
  doi          = {10.1007/S11704-024-3713-0},
  timestamp    = {Sun, 06 Oct 2024 21:27:53 +0200},
  biburl       = {https://dblp.org/rec/journals/fcsc/JiHZ24.bib},
  bibsource    = {dblp computer science bibliography, https://dblp.org}
}
```
