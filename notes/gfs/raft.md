### 如何处理leader和follower节点日志不一致的情况

**leader 从来不会覆盖或者删除自己的日志**，而是强制 follower 与它保持一致。



## 安全性

#### 要保证每个节点的状态机会严格按照相同的顺序 apply 日志

需要对“选主+日志复制”这套机制加上一些额外的限制，来保证**状态机的安全性**，也就是是 Raft 算法的正确性。

1. **对选举的限制**

我们再来分析下前文所述的 committed 日志被覆盖的场景，根本问题其实发生在第2步。Candidate 必须有足够的资格才能当选集群 leader，否则它就会给集群带来不可预料的错误。Candidate 是否具备这个资格可以在选举时添加一个小小的条件来判断，即：

**每个 candidate 必须在 RequestVote RPC 中携带自己本地日志的最新 (term, index)，如果 follower 发现这个 candidate 的日志还没有自己的新，则拒绝投票给该 candidate。**选主时比较的日志是不要求 committed 的，只需比较本地的最新日志就行

Candidate 想要赢得选举成为 leader，必须得到集群大多数节点的投票，那么**它的日志就一定至少不落后于大多数节点**。又因为一条日志只有复制到了大多数节点才能被 commit，因此**能赢得选举的 candidate 一定拥有所有 committed的 日志**。

因此Follower 不可能比 leader 多出一些 committed 日志。

比较两个 (term, index) 的逻辑非常简单：如果 term 不同 term 更大的日志更新，否则 index 大的日志更新。

2. **对提交的限制**

所谓 commit 其实就是对日志简单进行一个标记，表明其可以被 apply 到状态机，并针对相应的客户端请求进行响应。

**Leader 只允许 commit 包含当前 term 的日志。**之前term的日志只能在当前term的日志commit时一起commit。这样可以保证之前的term的日志要么不被commit，要么被commit后一定不会被覆盖，因为在它之后还有当前最新的term的日志，所以肯定不会有其他的没commit完日志的follower成功当选为新的leader，所以不会被覆盖。



以上两点能够保证每个节点会严格按照相同顺序commit日志





#### leader如何同步follower

在Raft中，没有明确的committed消息。相应的，committed消息被夹带在下一个AppendEntries消息中，由Leader下一次的AppendEntries对应的RPC发出。任何情况下，当有了committed消息时，这条消息会填在AppendEntries的RPC中。下一次Leader需要发送心跳，或者是收到了一个新的客户端请求，要将这个请求同步给其他副本时，Leader会将新的更大的commit号随着AppendEntries消息发出，当其他副本收到了这个消息，就知道之前的commit号已经被Leader提交，其他副本接下来也会执行相应的请求，更新本地的状态。





#### 故障产生时leader和follower如何同步

