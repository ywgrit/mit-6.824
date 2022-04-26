## lab2

#### 疑问

对appendentries的reply该做何处理

现在收到的reply如果显示错误，没有立即重新发送，如果reply延迟，也没有立即发送

为什么在使用channel时最好不要用锁



#### 应做的事

在src/raft/raft.go中增加代码



#### 如何测试

使用src/raft/test_test.go



#### 一些调试手段

提前结束程序运行

```go
return							//返回当前函数
exit 						//退出当前进程
runtime.Goexit() 				//提前退出当前go程，执行延迟调用
```

当想测试某个条件是否会在某一时刻被满足时，可以在判断语句的执行体中，打上显著的信息，并调用runtime.Goexit()来结束程序运行



#### 注意事项

代码不能有data race，否则通不过测试。自己测试时用go run -race来检测

raft实例只能够和rpc交互，不能使用文件或者go的共享变量

[一定要看看这个博客](https://www.inlighting.org/archives/mit-6.824-notes/#more)，写了很多应该注意的坑



#### 论文中应该注意的地方

- 在选举过程中，如果已经收到大多数follower的同意票，那么就停止向其他follower发送requestvote rpc，也不再接收到来的vote reply。candidate变成leader后，立即向各个节点发送heartbeat，然后结束。

- 在发送rpc并等待接收回复这段期间，发送者的各个状态可能已经改变了，要注意这一点。如果state改变了或者term改变导致处理reply的做法不同，需要严格注意这一点

- rpc应该怎么处理发送来的请求，要考虑到发送者希望接收到什么回复，发送者在接收到何种回复会做何种处理

- 一定要注意，在什么情况下，什么值应该被更新。这是实现中坑最多的地方

- 当follower收到过期的appendentries rpc时，应该直接丢弃这个rpc，过期是指leader的term小于follower的term。

- 无论什么情况下，只要对方发送过来的信息里面包含了对方的currentterm，就要将自己的currentterm和对方的currentterm做比较，当raft server发现别人的term比自己大时，就要将自己的term更新为这个新的较大值。并且要将自己转换为follower，votedfor设置为-1，紧接着再进行一次持久化。为了不漏掉某些操作，最好将这些操作打包成一个函数updateterm，遇到这个情况时，直接调用updateterm。
- 在选举时，如果某个候选人在appendentries函数中发现合法的leader给自己发了消息，即leader的Index大于等于自己的Index，自己也就没必要竞选leader了。要将自己转换为follower，并且votedfor设置为-1，紧接着再进行一次持久化。
- 每次一个新的leader当选成功后，都要执行一次心跳检测，为了通知所有server自己是服务器，以及为了避免其他服务器发起新的选举
- 除了每次执行心跳检测后，不用手动重置heartbeattimeout，因为心跳检测是必须每个一段时间就执行一次的。而follower之所以会变成candidate来试图将自己选举为新的leader，就是因为自己长时间没有收到leader的信息，或者candidate的信息。所以除了每次执行选举后，eletiontimeout还要在两种情况下需要重置：1、收到leader的信息后，2、candidateA发送过来选票，并且自己同意为A投票。

- 对于过期请求的回复，直接抛弃就行

- 当选出一个新的leader时，nextIndex[]和matchIndex[]应该被重置，nextIndex[]应该被重置为rf.LastLog().Index+1，matchIndex[]应该被重置为0，matchIndex[leaderId]应该被leader置为rf.LastLog().Index

- leader会强制follower复制自己的log entry。当执行appendentries和server i发生冲突时，leader的matchIndex[i]会被置为0，nextIndex[i]会不断递减，直到在nextIndex[i]位置上leader和server i的term相同，接着再往后复制。appendentries成功后，leader的matchIndex[i]会被置为server i 的rf.LastLog().Index

- log[0]并没有任何意义，用来存放最后一条log的index

- leader会不断向所有的follower同步log，即使leader已经回应了客户端

- commited log entry会被所有server执行

- raft一致性：1、如果两个server的两个log entry有相同的index和term，那么这两个log entry存储着相同的命令，2、如果在两个server上的两个log entry有相同的index和term，那么这两个log entry之前的log entry也是相同的

- 选举时，more up-to-date是使用最后一条log entry判断的，无论这个entry是否被提交

- 何时应该重置选举时间？

  收到appendentries rpc时，如果rpc过期了，那么就直接返回错误。否则需要重置选举时间（electiontimeout）

  收到requestvote rpc，如果同意投票，那么需要重置electiontimeout

  收到InstallSnapshot rpc

  raft server自己开始了一次选举（选举函数是快速返回的，因为在函数里面开启了协程）后，需要重置electiontimeout，这是为了避免在自己选举过程中，因为网络问题，导致rpc被延迟或者被丢弃了，从而导致一直没能成功选举出一个新的leader。这时因为在开始选举后，重置了electiontimeout，所以会开始一轮新的选举。

- 因为只有上述几个地方重置了选举时间，所以leader节点也会重置

- 锁的使用：发送 rpc，接收 rpc，push channel，receive channel 的时候一定不要持锁，否则很可能产生死锁。这一点 locking 博客里面有介绍到。当然，基本地读写锁使用方式也最好注意一下，对于仅读的代码块就不需要持写锁了。

- appendentries的过期的回复应该直接丢弃，两种情况下会过期：1、leader A 向 follower B发送了appendentries rpc，在等待B回复的时候，A的tcurrenTerm已经改变了（因为其他成员没收到A的消息，其他成员选出了一个新的leader，然后又重新选了）



#### 数据结构

leader和follower都用Raft结构体表示，而不是用两种结构体表示，所以Raft结构体中既要包含leader的成员，也要包含follower的成员。只是在leader中，不会用到follower的成员；在follower中，不会用到leader的成员

heartbeat也是通过appendentry rpc实现的。

不同之处：

- 真正的appendentry需要等待replicatorCond条件变量被唤醒，在客户端向leader发送执行命令时，replicatorCond条件变量会被唤醒。heartbeat不需要，直接发送检测信息就行，所以需要在replicator函数中嵌套replicate函数（真正执行appendentry的函数），并且在replicator中要等待replicatorCond条件变量被唤醒，被唤醒后再执行replicate函数，heartbeat函数中也会调用replicate函数。但不需要等待任何条件变量的唤醒，直接调用replicate函数。
- 普通的appendentry的appendentryarg的各个成员都是有意义的，都需要被赋予一定的值。而在heartbeat中，entries[]为空。

heartbeat和真正的appendentry没有任何区别，都是调用replicate。在heartbeat中调用replicate函数时，entries[]会自动被设置为空。在heartbeat中也会检查conflicting logentries，也会比较leadercommit和rf.commitIndex，因为follower中可能存在未commit的logentries，要等leadercommit后，follower才能提交



每个raft在创建时都会开启一些后台协程：

- ticker：一个，定时启动选举过程和心跳检测
- applier：一个，向applyCh传送commited LogEntry。创建时会阻塞住，等待commit LogEntry事件唤醒。每次leader的commitIndex改变了之后，都要检查commitindex > lastapplied条件是否满足，如果满足，就要唤醒leader的applycond
- replicator：len(peer) - 1个，只在leader当中用到了，向所有的follower复制LogEntry。replicator(i)会将leader新接收到的LogEntry复制到follower中。创建时会阻塞住，等待leader接收到客户端请求时唤醒。每次收到客户端的命令（start函数中接收客户端命令）时都会唤醒replicatorCond[i]。

```go 
for i := range rf.peers { //只有leader才会执行这段代码
	if i == rf.me { // leader的replicatorCond不能被唤醒
		continue
	}
	rf.replicatorCond[i].Signal()
}
```



```go
type Raft struct {
	applyCh 		chan ApplyMsg // raft通过这个channel向上层server传递信息
    applyCond 		*sync.Cond // applier协程会等待这个变量被commit LogEntry事件唤醒
    replicatorCond []*sync.Cond //leader的replicator(i)会等待replicatorCond[i]这个条件变量被"leader接收到客户请求"这一事件唤醒
}
```

#### 2A（leader election）

参考论文图二

每个raft server的睡眠时间应该随机

leader失败或者与其他服务器网络不通后，需要选出一个新的leader

**持久化时机**

如果Leader收到了一个客户端请求，在发送AppendEntries RPC给Followers之前，必须要先持久化存储在本地。因为Leader必须要commit那个请求，并且不能忘记这个请求。实际上，在回复AppendEntries 消息之前，Followers也需要持久化存储这些Log条目到本地，因为它们最终也要commit这个请求，它们不能因为重启而忘记这个请求。

总的来说，Raft结构体中，currentTerm、votedFor、log[]三个成员中任意一个改变了后，都需要持久化

**持久化注意事项**

write函数并不意味着真的会将文件内容冲刷到磁盘上，fsync()能够保证将文件内容冲刷到磁盘上，但是这个系统调用的代价很高，尽量减少它的使用。

**选举超时时间和心跳超时时间**

如果follower探测不到leader的存在，那么follower在超时时间后会转化为candidate，所以leader每间隔一段时间就需要向所有follower发送信息，这段间隔就是心跳超时时间。

发送心跳检测和appendentry

选举成功后必须进行一次心跳检测



#### 2B

注意：commit了不一定apply了。举个例子：commitIndex为10，lastAppliedIndex为10，这个时候新的commitIndex出来了，比如说15，这个时候你需要应用11～15的log到状态机中，应用完毕之后lastAppliedIndex就变为15。

raft appendentries分为三个阶段

第一阶段：master收到client请求，记录log, 发送复制到其他slave 。一半以上slave回复ack后，进入第二阶段：master发送commitindex给各个slave， slave移动自己的commit index。第三阶段：每个salve执行这些commit的log，移动applied index到最新的commit index位置。

leader不仅包括commitindex和lastapplied，还包含nexindex[]和matchindex[]。



#### 2D

每个服务器都单独对log进行快照，每进行一次快照，快照之前的logentries和之前的旧快照就会被抛弃。

因为可能会存在这样一种情况：leader对某个logentry之前的所有logentries进行了快照，其中有些logentries因为网络问题未能成功发送到follower中，或者一些follower刚刚加入进来，那么这些follower就没包含这些logentries，所以leader需要时不时地向follower发送快照以进行同步。

进行快照的logentry都是已经committed的，leader通过installsnapshot rpc将snapshot发送给follower后，follower会根据args.term是否小于follower.term来判断是否保存snapshot。如果需要保存，只会保存snapshot，并且将snapshot应用到follower.applyCh中，不会install snapshot，这一步由condinstallsnapshot函数完成







### 数据

#### 测试程序运行时间

|        | old      | new_with_replicateCond | new_without_replicateCond |
| ------ | -------- | ---------------------- | ------------------------- |
| 2A     | 16.029s  | 15.322s                | 14.642s                   |
| 2B     | 34.599s  | 35.107s                | 36.169s                   |
| 2C     | 122.308s | 195.586s               | 126.297s                  |
| 2D     | 162.398s | 168.719s               | 150.918s                  |
| 2total | 335.334s | 414.734s               | 328.026s                  |
| entire | 333.730s | 461.977s               | 323.316s                  |

