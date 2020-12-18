package xntural.cn.remoter;

import cn.xnatural.aio.AioBase;
import cn.xnatural.aio.AioClient;
import cn.xnatural.aio.AioServer;
import cn.xnatural.aio.AioStream;
import cn.xnatural.enet.event.EC;
import cn.xnatural.enet.event.EL;
import cn.xnatural.enet.event.EP;
import cn.xnatural.sched.Sched;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * 集群分布式 核心类
 * 多应用之间的连接器,简化跨系统调用
 * 依赖 {@link AioServer}, {@link AioClient}
 */
public class Remoter extends AioBase {
    protected final static Logger                      log = LoggerFactory.getLogger(Remoter.class);
    /**
     * 属性集
     */
    protected              Map<String, Object>         attrs;
    /**
     * 线程池
     */
    protected              ExecutorService             exec;
    /**
     * 事件中心
     * https://gitee.com/xnat/enet
     */
    protected              EP                          ep;
    /**
     * 定时任务
     * https://gitee.com/xnat/sched
     */
    protected              Sched                       sched;
    /**
     * 应用名
     */
    protected              String                      appName;
    /**
     * 应用实例id
     */
    protected              String                      appId;
    /**
     * 保存集群中的应用节点信息
     * name -> Node
     */
    protected final        Map<String, SafeList<Node>> nodeMap        = new ConcurrentHashMap<>();
    /**
     * 远程事件临时持有
     * ecId -> {@link EC}
     */
    protected final        Map<String, EC>             ecMap          = new ConcurrentHashMap<>();
    /**
     * 集群数据版本管理
     */
    protected final Map<String, DataVersion> dataVersionMap = new ConcurrentHashMap<>();
    /**
     * tcp数据发送端
     */
    protected AioClient                      aioClient;
    /**
     * tcp数据接收端
     */
    protected AioServer                      aioServer;
    /**
     * 集群的服务中心地址 [host1]:port1,[host2]:port2. 例 :8001 or localhost:8001
     */
    protected final LazySupplier<String>           _masterHps     = new LazySupplier(() -> attrs.getOrDefault("masterHps", null));
    /**
     * 是否为master
     * true: 则向同为master的应用同步集群应用信息, false: 只向 masterHps 指向的服务同步集群应用信息
     */
    protected final LazySupplier<Boolean>          _master        = new LazySupplier(() -> attrs.getOrDefault("master", false));
    /**
     * 数据编码
     */
    protected final LazySupplier<String>           _charset       = new LazySupplier(() -> attrs.getOrDefault("charset", "utf-8"));


    public Remoter(String appName, String appId, Map<String, Object> attrs, ExecutorService exec, EP ep, Sched sched) {
        if (appName == null || appName.isEmpty()) throw new IllegalArgumentException("appName must not be empty");
        this.appName = appName;
        this.appId = appId == null || appId.isEmpty() ? UUID.randomUUID().toString().replace("-", "") : appId;
        this.attrs = attrs == null ? new ConcurrentHashMap<>() : attrs;
        this.exec = exec == null ? Executors.newFixedThreadPool(4, new ThreadFactory() {
            AtomicInteger i = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "remoter-" + i.getAndIncrement());
            }
        }) : exec;
        this.ep = ep == null ? new EP(exec) : ep;
        this.ep.addListenerSource(this);
        this.sched = sched == null ? new Sched() : sched;
        this.aioClient = new AioClient(attrs, exec);
        this.aioServer = new AioServer(attrs, exec);
        this.aioServer.start();
    }


    /**
     * stop
     */
    public void stop() {
        aioClient.stop();
        aioServer.stop();
    }


    /**
     * 同步远程事件调用
     * @param appName 应用名
     * @param eName 远程应用中的事件名
     * @param remoteMethodArgs 远程事件监听方法的参数
     * @return 事件执行结果值
     */
    public Object fire(String appName, String eName, List remoteMethodArgs) {
        CountDownLatch latch = new CountDownLatch(1);
        EC ec = EC.of(this).args(appName, eName, remoteMethodArgs).completeFn(ec1 -> latch.countDown());
        ep.fire("remote", ec); // 不能直接用doFire, 因为 isNoListener() == true
        try {
            latch.await();
            if (ec.isSuccess()) return ec.result;
            else throw new RuntimeException(ec.failDesc());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 异步远程事件调用
     * @param appName 应用名
     * @param eName 远程应用中的事件名
     * @param callback 回调函数. 1. 正常结果, 2. Exception
     * @param remoteMethodArgs 远程事件监听方法的参数
     */
    void fireAsync(String appName, String eName, Consumer callback, List remoteMethodArgs) {
        ep.fire("remote", EC.of(this).args(appName, eName, remoteMethodArgs).completeFn(callback != null ?  ec -> {
            if (ec.isSuccess()) callback.accept(ec.result);
            else callback.accept(new RuntimeException(ec.failDesc()));
        }: null));
    }


    /**
     * 向应用其中一个实例发送数据
     * @param appName 应用名
     * @param msg 消息
     * @return
     */
    public Remoter sendMsgToAny(String appName, String msg) {
        sendMsg(appName, msg, "any");
        return this;
    }


    /**
     * 向不应用的所有实例发送消息
     * @param appName 应用名
     * @param msg 消息
     * @return
     */
    public Remoter sendMsgToAll(String appName, String msg) {
        sendMsg(appName, msg, "all");
        return this;
    }


    /**
     * 发送消息
     * @param appName 应用名
     * @param msg 消息内容
     * @param target 可用值: 'any', 'all'
     */
    protected void sendMsg(String appName, String msg, String target) {
        final SafeList<Node> nodes = nodeMap.get(appName);
        if (nodes == null || nodes.isEmpty()) {
            throw new RuntimeException("Not found app '" + appName + "' system online");
        }

        final BiConsumer<Node, BiConsumer<Exception, Node>> doSend = (node, failFn) -> {
            if (node == null) return;
            String[] arr = node.tcp.split(":");
            try {
                aioClient.send(arr[0], Integer.valueOf(arr[1]), msg.getBytes(_charset.get()), ex -> {
                        log.error("Send fail msg: " + msg + " to Node: " + node, ex);
                        if (failFn != null) failFn.accept(ex, node);
                    }, se -> {
                        log.trace("Send success msg: {} to Node: {}", msg, node);
                });
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        };

        final Predicate<Node> predicate = node -> !Objects.equals(node.id, appId);
        if ("any".equals(target)) {
            final BiConsumer<Exception, Node> failFn = new BiConsumer<Exception, Node>() {
                @Override
                public void accept(Exception e, Node n) {
                    if (nodes.size() <= 1) return;
                    // 如果有多个(>1)节点, 则删除失败的节点
                    nodes.remove(n);
                    Node node = nodes.findRandom(predicate);
                    if (node != null) doSend.accept(node, this);
                }
            };
            doSend.accept(nodes.findRandom(predicate), failFn);
        } else if ("all".equals(target)) { // 网络不可靠
            nodes.withReadLock(() -> {
                nodes.iterator().forEachRemaining(node -> {
                    if (predicate.test(node)) {
                        exec.execute(() -> doSend.accept(node, null));
                    }
                });
            });
        } else {
            throw new IllegalArgumentException("Not support target '" + target + "'");
        }
    }


    /**
     * 获取数据集的数据版本集群同步器
     * @param key 同步器的key
     * @return {@link DataVersion}
     */
    public DataVersion dataVersion(String key) {
        if (key == null || key.isEmpty()) throw new IllegalArgumentException("dataVersion key must not be empty");
        return dataVersionMap.computeIfAbsent(key, s -> new DataVersion(this, key));
    }


    /**
     * 接收远程远程应用的数据同步
     * @param key 同步Key
     * @param dataKey 数据key
     * @param version 数据版本
     * @param data 数据
     */
    @EL(name = "dataVersion")
    protected void receiveDataVersion(String key, String dataKey, Long version, Object data) {
        dataVersionMap.computeIfAbsent(key, s -> new DataVersion(this, key)).receive(dataKey, version, data);
    }


    /**
     * 集群应用同步函数
     * 监听系统心跳事件, 随心跳去向master同步集群应用信息
     */
    @EL(name = "sys.heartbeat", async = true)
    public void sync() {
        try {
            if (_masterHps.get() == null || _masterHps.get().isEmpty()) {
                // log.warn("'${name}.masterHps' not config");
                return;
            }
            doSyncFn.run();
            // lastSyncSuccess = System.currentTimeMillis()
        } catch (Exception ex) {
            sched.after(Duration.ofSeconds(getInteger('errorUpInterval', 30) + new Random().nextInt(60)), {sync()})
            log.error("App up error. " + (ex.message?:ex.class.simpleName), ex)
        }
    }




    /**
     * 调用远程事件
     * 执行流程: 1. 客户端发送事件:  {@link #doFire(EC, String, String, List)}
     *          2. 服务端接收到事件: {@link #receiveEventReq(com.alibaba.fastjson.JSONObject, cn.xnatural.aio.AioStream)}
     *          3. 客户端接收到返回: {@link #receiveEventResp(com.alibaba.fastjson.JSONObject)}
     * @param ec
     * @param appName 应用名
     * @param eName 远程应用中的事件名
     * @param remoteMethodArgs 远程事件监听方法的参数
     */
    @EL(name = "remote")
    protected void doFire(EC ec, String appName, String eName, List remoteMethodArgs) {
        if (aioClient == null) throw new RuntimeException("aioClient not is running");
        if (appName == null) throw new IllegalArgumentException("appName is empty");
        ec.suspend();
        JSONObject params = new JSONObject(5, true);
        try {
            boolean trace = (ec.isTrack() || getBoolean("trace_*_*", false) || getBoolean("trace_*_" + eName, false) || getBoolean("trace_" + appName + "_*", false) || getBoolean("trace_" +appName+ "_" + eName, false));
            if (ec.id() == null) {ec.id(UUID.randomUUID().toString().replaceAll("-", ""));}
            params.put("id", ec.id());
            boolean reply = (ec.completeFn() != null); // 是否需要远程响应执行结果(有完成回调函数就需要远程响应调用结果)
            params.put("reply", reply);
            params.put("name", eName);
            // params.put("trace", trace)
            if (remoteMethodArgs != null && !remoteMethodArgs.isEmpty()) {
                JSONArray args = new JSONArray(remoteMethodArgs.size());
                params.put("args", args);
                for (Object arg : remoteMethodArgs) {
                    if (arg == null) args.add(new JSONObject(0));
                    else args.add(new JSONObject(2).fluentPut("type", arg.getClass().getName()).fluentPut("value", arg));
                }
            }
            if (reply) {
                ecMap.put(ec.id(), ec);
                // 数据发送成功. 如果需要响应, 则添加等待响应超时处理
                sched.after(Duration.ofSeconds(
                        (long) ec.getAttr("timeout", Integer.class, getInteger("timeout_" + eName, getInteger("eventTimeout", 10)))
                ), () -> {
                        EC ecc = ecMap.remove(ec.id());
                        if (ecc != null) ecc.errMsg("'" +appName+ "_'" +eName+ " Timeout").resume().tryFinish();
                });
                if (trace) {
                    Consumer<EC> fn = ec.completeFn();
                    ec.completeFn((ec1) -> {
                        if (ec.isSuccess()) {
                            log.info("End remote event. id: "+ ec.id() + ", result: " + ec.result);
                        }
                        fn.accept(ec);
                    });
                }
            }

            // 发送请求给远程应用appName执行. 消息类型为: 'event'
            JSONObject data = new JSONObject(3)
                    .fluentPut("type", "event")
                    .fluentPut("source", new JSONObject(2).fluentPut("name", appName).fluentPut("id", appId))
                    .fluentPut("data", params);
            if (ec.getAttr("toAll", Boolean.class, false)) {
                if (trace) {log.info("Fire Remote Event(toAll). app: "+ appName +", params: " + params);}
                sendMsgToAll(appName, data.toString());
            } else {
                if (trace) {log.info("Fire Remote Event(toAny). app: "+ appName +", params: " + params);}
                sendMsgToAny(appName, data.toString());
            }
        } catch (Throwable ex) {
            log.error("Error fire remote event to '" +appName+ "'. params: " + params, ex);
            ecMap.remove(ec.id());
            ec.ex(ex).resume().tryFinish();
        }
    }


    /**
     * 接收远程发送的消息
     * @param msg 消息内容
     * @param stream AioStream
     */
    protected void receiveMsg(final String msg, final AioStream stream) {
        JSONObject msgJo = null;
        try {
            msgJo = JSON.parseObject(msg, Feature.OrderedField);
        } catch (JSONException ex) { // 不处理非json格式的消息
            return;
        }

        String t = msgJo.getString("type");
        if ("event".equals(t)) { // 远程事件请求
            JSONObject sJo = msgJo.getJSONObject("source");
            if (sJo == null || sJo.isEmpty()) {
                log.warn("Unknown source. origin data: " + msg);
                stream.close(); return;
            }
            if (appId.equals(sJo.getString("id"))) {
                log.warn("Not allow fire remote event to self");
                stream.close(); return;
            }
            receiveEventReq(msgJo.getJSONObject("data"), stream);
        } else if ("appUp".equals(t)) { // 应用注册在线通知
            JSONObject sJo = msgJo.getJSONObject("source");
            if (sJo == null || sJo.isEmpty()) {
                log.warn("Unknown source. origin data: " + msg);
                stream.close(); return;
            }
            if (appId.equals(sJo.getString("id"))) {
                log.warn("Not allow register up to self");
                stream.close(); return;
            }
            queue(APP_UP) {
                JSONObject d = null;
                try { d = msgJo.getJSONObject("data"); appUp(d, stream); }
                catch (Exception ex) {
                    log.error("App up error!. data: " + d, ex);
                }
            }
        } else if ("cmd-log".equals(t)) { // telnet 命令行设置日志等级
            // telnet localhost 8001
            // 例: {"type":"cmd-log", "data": "core.module.remote: debug"}
            String[] arr = msgJo.getString("data").split(":");
            // Log.setLevel(arr[0].trim(), arr[1].trim())
            //stream.reply("set log level success".getBytes(_charset.get()));
        } else if (t && t.startsWith("ls ")) {
            // {"type":"ls apps"}
            def arr = t.split(" ")
            if (arr?[1] = "apps") { // ls apps
                stream.reply(JSON.toJSONString(nodeMap).getBytes("utf-8"));
            } else if (arr?[1] == 'app' && arr?[2]) { // ls app gy
                stream.reply(JSON.toJSONString(nodeMap[arr?[2]]).getBytes(_charset.get()));
            }
        } else {
            log.error("Not support exchange data type '{}'", t)
            stream.close()
        }
    }
}
