collectl-lustre - Collectl Lustre Data Collection Plugins
============================================================

- The Collectl Lustre plugins (lustreMDS, lustreOSS, and lustreClient) are based on the built-in Collectl 3.6.9 Lustre collector

- The plugins add support for Lustre 2.x (up to 2.5.x tested), and remove support for Lustre versions pre 1.8 and HPSFS

- The plugins add new data collection options. Most of the new metrics are only available via Ganglia export; however, I am more than happy to work with users on extending the console reports and plots

- Please report all bugs and enhancement requests to ppiela@cray.com

- You should not use -sl or -sL in combination with the plugins

- If you run collectl as a daemon you will have to add -l to the subsytem option because in daemon-mode collectl automatically enables the builtin Lustre data collector. Your subsytem definition will look something like -s-lYZ

- The following table compares the capabilities and command options of the plugins with the built-in Lustre collector in collectl 3.6.9. 


| Type | Level | 3.6.9 command | Plugin command | 3.6.9 output | Plugin output |
| ---- | ----- | ------------- | -------------- | ------------ | ------------- |
| MDS/MDT | Summary | collectl -sl | collectl.pl --import lustreMDS,s | Gattr+, Sattr+, Sync, Unlnk, Create | Same as 3.6.9 |
| | Verbose | collectl -sl --verbose | collectl.pl --verbose --import lustreMDS,s | Getattr, attrLck, StatFS, Sync, Gxattr, Sxattr, Connect, Disconn, Create, Link, Setattr, Rename, Unlink | Same as 3.6.9 with the following additions: FCreate, Open, Close, Mkdir, and Rmdir |
| | Detailed | collectl -sL --lustopts D (only for HPSFS) | Not supported |
| | lustopts: C | collectl.pl --import lustreMDS,sC | | Collects metadata operations by client. Currently only reported via Ganglia export. The following statistics are exported: <client ip address>@<network interface>.close, create, getattrP, open, sattrP, unlink |
OSS/OST	| Summary | collectl -sl | collectl.pl --import lustreOSS,s | KBRead, Reads, KBWrit, Writes | Same as 3.6.9
| | Verbose | collectl -sl --verbose | collectl.pl --import lustreOSS,s --verbose | KBRead, Reads, SizeKB, KBWrite,Writes, SizeKB | Same as 3.6.9
| | Detailed | collectl -sL | collectl.pl --import lustreOSS,d | Ost, KBRead, Reads, SizeKB, KBWrite, Wraites, SizeKB | Same as 3.6.9
| | lustopts:B | collectl -sl --lustopts B | collectl.pl --import lustreOSS,sB | RdK, Rds, 1P, 2P, 4P, 8P, 16P, 32P, 64P, 128P, 256P, WrtK, Wrts, 1P, 2P, 4P, 8P, 16P, 32P, 64P, 128P, 256P | Same as 3.6.9. Disk I/O size stats are also collected, but are currently only reported via Ganglia export. The following statistics are exported: lustost.diskIoSizeRead.<ost-name>.[0004k-1024k] |
| | lustopts:C | collectl.pl --import lustreOSS,sC | Collects read/write throughput (Bytes/sec) by client. Currently only reported via Ganglia export. The following statistics are exported:<client ip address>@<network interface>.read, write |
| Client | Summary | collectl -sl | collectl.pl --import lustreClient,s | KBRead, Reads, KBWrite, Writes | Same as 3.6.9 |
| | Verbose | collectl --verbose -sl | collectl.pl --verbose --import lustreClient,s | KBRead, Reads, SizeKB, KBWrite, Writes, SizeKB | Same as 3.6.9 |
| | Detailed | collectl -sL | collectl.pl --import lustreClient,d | Filsys, KBRead, Reads, SizeKB, KBWrite, Writes, SizeKB | Same as 3.6.9 |
| | lustopts:B | collectl -sl --lustopts B | collectl.pl --import lustreClient,sB | RdK, Rds, 1P, 2P, 4P, 8P, 16P, 32P, 64P, 128P, 256P | Same as 3.6.9 |
| | lustopts:M | collectl -sl --lustopts M | collectl.pl --import lustreClient,sM | KBRead, Reads, KBWrite,Writes, Open, Close, GAttr, SAttr, Seek, Fsync, DrtHit, DrtMis | Same as 3.6.9 |
| | lustopts:R | collectl -sl --lustopts R | collectl.pl --import lustreClient,sR | KBRead, Reads, KBWrite, Writes, Hits, Misses | Same as 3.6.9 |
| | lustopts:R | collectl -sL --lustopts R | collectl.pl --import lustreClient,dR | Filsys, KBRead, Reads, SizeKB, KBWrite, Writes, SizeKB, Pend, Hits, Misses, NotCon, MisWin, FalGrb, LckFal, Discrd, ZFile, ZerWin, RA2Eof, HitMax, Wrong | Same as 3.6.9 |


