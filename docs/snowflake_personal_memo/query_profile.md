(1) Total Execution Time

(2) Processing %
CPU Time. If the number is high you probably have a lot of filters, big joins, trnasformations etc..

(3) Local Disk I/O %
Waiting time on local disk, if high it means the request used all the available memory.
The data is spilling to the local disk and the request will take more time.

(4) Remote Disk I/O %
Waiting time on remote disk, if high means the request used all the available memory and is now spilling dat aon the remote disk which have a speed usually 10x times lower.

(5) Initialization %
Time to prepare / initialize the request. Too high means complex request with many subqueries , useless logic.

Synchronizing %
Coordinating the process, exhcange of data.

(6) Scan Progress
Usefull if request still running, can see the time spent and how lmuch time remaining.

(7) Bytes Scanned
The data volume read. The number give an idea of how much we pay as reading.

(8) Percentage Scanned from Cache
Part of data read from cache.

(9) Partitions Scanned
Number of micro-partitions read.

(10) Partitions Total
Total of micro-partitions available from the read tables. If ratio is weak the less Snwowflake read micro-partitions.