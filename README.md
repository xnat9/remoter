## 介绍
轻量级远程调用工具. 基于 [enet](https://gitee.com/xnat/enet) 和 [aio](https://gitee.com/xnat/aio)

往往一个大的系统中由多个小的应用组成, 小应用之间的交互一般都是http

remoter把交互方式变为tcp, 串联集群中所有应用, 让应用与应用之间的交互简单

例:
```
// 触发应用名为 gy 中的事件 eName1, 参数为: p1
Object result = remoter.fire("gy", "eName1", Arrays.asList("p1"))
```

## 安装教程
```
<dependency>
    <groupId>cn.xnatural.remoter</groupId>
    <artifactId>remoter</artifactId>
    <version>1.0.2</version>
</dependency>
```

## 应用中加入Remoter

masterHps: 集群的服务中心地址 host1:port1,host2:port2 ...

hp: 暴露给集群之间通信 ip和端口. 例: ':7001', 或者 'localhost:7001'

master: 是否为服务中心
```
Map<String, Object> attrs = new HashMap<>();
attrs.put("masterHps", "xnatural.cn:8001");
attrs.put("hp", ":7001");
attrs.put("master", true);
Remoter remoter = new Remoter("应用1", "实例id1", attrs);
remoter.autoHeartbeat(); // 自动开始向 masterHps 同步集群中所有应用的信息
// remoter.sync(); // 手动触发同步
```

```
// 暴露给集群的其它事件源
remoter.getEp().addListenerSource(new Object() {
    @EL(name = "xx", async = true)
    String xx() {
        return "oo";
    }
});

// 其它应用调用
Object result = remoter.fire("应用名1", "xx"); // result == "oo"
```
## 远程事件参数类型
String, Boolean, Short, BigInteger, Integer, Long, Double, Float, BigDecimal, URI


## 异步事件调用
```
Object result = remoter.fireAsync("gy", "eName1", (result) -> { //回调函数
    // TODO 处理返回结果
}, Arrays.asList("p1"))
```

## 版本数据
提供相同应用多实例之前共享数据/数据同步的功能

当一个实例更新版本数据时, 执行update 会自动同步到其它实例

多个实例连接相同数据库, 当其中一个实例更新数据库时, 执行update通知其它实例

典型应用: 多个后台管理应用实例之间同步数据 [例](https://gitee.com/xnat/gy/blob/rule/src/service/rule/AttrManager.groovy#L191)
```
remoter.dataVersion("同步key").update("数据key", 数据版本(Long), 数据);
```

## 参与贡献
xnatural@msn.cn
