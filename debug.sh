#!/usr/bin/env bash
#!/bin/bash
#

# if [[ ! -d /mnt/huge ]]; then
# 	#statements
# 	sudo mkdir -p /mnt/huge
# 	sudo mount -t hugetlbfs nodev /mnt/huge
# 	sudo echo 256 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages
# fi


ninja -j 2 release && sudo gdb --args build/release/apps/imdb/imdb -c 1 -m 256M $@
