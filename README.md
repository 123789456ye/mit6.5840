mit6.5840 Spring 2025 Lab1-5 Code

Pass all test once

使用AI辅助Debug，所以会有一些注释

## Lab 1 MapReduce

对着论文写，学习Go

## Lab 2 K-V Server

对着论文写，学习rpc，跟着Hint写就行

## Lab 3 Raft

### 3A+3B

Raft的框架，对着论文图2写

有时候调调超时和心跳间隔有奇效

### 3C

把persist加上就行，最简单的一集

### 3D

这才是真正的多线程调试！

花的时间和3A+B+C差不多，一开始调红温了，直接切到3C花了一天重新写一遍，结果还是错的，更红温了

首先有个很经典的死锁问题，和测试代码和上层服务器有关，建议去查一下，当然像我这样花一天debug并且修好也行

然后是各种边界问题，就不说了

最后是要确保把前面的框架代码所有Index全都改成快照版本，少改一个就不知道会出什么问题

事实上这个代码还是有一些问题的，多测会挂掉，而且性能比标称也差1.5倍的样子，不过实在是懒得修了

## Lab 4 FT K-V Server

### 4A

貌似是今年新加的，没有参考，不过也不难

注意超时的情况，读下测试代码就行

### 4B

逻辑和Lab 2 差不多，大部分都能抄

速度测试是80+ms/op，通过要到33，参考值是16，难绷

还要回去优化Raft层，不打算修了，其它的都过了

### 4C

和Lab 3 异曲同工，逻辑本身很简单，但是要回去修Raft层的Bug

改完一个还得重新跑3C的测试，不然改完几个再跑出问题就 噔 噔 咚

不过这次倒还好，结论来说，在多服务器的情况下getbyindex那里还是不能直接减掉，而还是要二分

明明之前在3D的时候特意测试了几次，减掉和二分的结果全都是一致的，不知道是哪里写错了

速度测试70+ms/op，通过要到33，不修了

TestSnapshotUnreliable4C 会直接卡到10分钟强制退出，发现是并发压力太大了，还是得修Raft层，一测试发现Raft层挂了，遂红温，回退直接测试，突然又过了，测了几次都过了，神秘

可能是在Start那里要不要直接启一个sendHeartbeat，不启速度会掉到120ms/op，前面的测试也有可能有问题，起了RPC会爆炸，不过定位了就不修了算了

## Lab 5 Shared K-V Server

### 5A

要改4个文件，最脑袋疼的一集

不知道为什么关掉之后再启动会出问题，找了半天愣是没找到，也没想明白怎么会在这上面出问题，先放会再修

静态测试和后面的测试都能过，其它的逻辑应该是对的

### 5B+C

这里其实没有太多要注意的，就是map之类的比较大的东西有可能会触发多线程race
