## 简介



## 设计概要

### 架构

客户端不缓存数据文件，因为数据文件一般都非常大，应用程序访问的文件量一般远远大于客户端的cache容量。

客户端会缓存metadata

chunkserver不会缓存数据文件，因为文件是存储在chunkserver本地上的，linux的buffer cache已经缓存了数据文件。

GFS采用弱一致性，所以不同chunkserver上的数据可能略微有所不同



### master

客户端使用k-v对缓存metadata信息，key为文件名和chunk index，value为chunk位置（即chunk服务器列表）和 chunk handle（chunk ID）。每个k-v对都是有过期时间的

metadata包含三类信息：文件和chunk的命名空间、文件名到chunks的映射关系、每个chunk的位置。前两类信息都会被持久化到master的磁盘上，这样以防master崩溃而导致信息丢失。chunk位置不会被持久化，每次master和chunkserver启动时都会重新记录

当Master节点故障重启，并重建它的状态时，不会从log的最开始重建状态，因为log的最开始可能是几年之前，所以Master节点会在磁盘中创建一些checkpoint点，可能花费一分钟左右的时间。这样Master节点重启时，会从log中的最近一个checkpoint开始恢复，再逐条执行从Checkpoint开始的log，最后恢复自己的状态





#### 读文件

对于读文件来说，可以从任何最新的Chunk副本读取数据

客户端从master中获取到chunk id和chunkserver列表信息后，会从最近的chunkserver当中读取数据

#### 写文件

对于写文件来说，必须要通过Chunk的主副本（Primary Chunk）来写入。对于某个特定的Chunk来说，在某一个时间点，Master不一定指定了Chunk的主副本。所以，写文件的时候，需要考虑Chunk的主副本不存在的情况，这时Master节点需要能够在Chunk的多个副本中识别出，哪些副本是新的，哪些是旧的，把最新的副本作为主副本

所有的Secondary都有相同的版本号。版本号只会在Master指定一个新Primary时才会改变。通常只有在原Primary发生故障了，才会指定一个新的Primary。所以，副本（参与写操作的Primary和Secondary）都有相同的版本号

当写入失败，客户端重新发起写入数据请求时，客户端会从整个流程的最开始重发。客户端会再次向Master询问文件最后一个Chunk是什么，因为文件可能因为其他客户端的数据追加而发生了改变。