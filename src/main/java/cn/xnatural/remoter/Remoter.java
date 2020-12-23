package cn.xnatural.remoter;

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
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 基于 https:/gitee/xnat/enet 做的远程事件
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
    protected final LazySupplier<Boolean>          _master        = new LazySupplier(() -> Boolean.valueOf(attrs.getOrDefault("master", false).toString()));
    /**
     * 数据编码
     */
    protected final LazySupplier<String>           _charset       = new LazySupplier(() -> attrs.getOrDefault("charset", "utf-8"));


    /**
     *
     * @param appName 当前应用名
     * @param appId 当前应用实例id
     * @param attrs 属性集
     * @param exec 线程池
     * @param ep 事件中心
     * @param sched 定时任务调度
     */
    public Remoter(String appName, String appId, Map<String, Object> attrs, ExecutorService exec, EP ep, Sched sched) {
        super(attrs, exec);
        if (appName == null || appName.isEmpty()) throw new IllegalArgumentException("appName must not be empty");
        this.appName = appName;
        this.appId = appId == null || appId.isEmpty() ? UUID.randomUUID().toString().replace("-", "") : appId;
        this.attrs = attrs == null ? new ConcurrentHashMap<>() : attrs;
        this.exec = exec == null ? Executors.newFixedThreadPool(4, new ThreadFactory() {
            AtomicInteger i = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) { return new Thread(r, "remoter-" + i.getAndIncrement()); }
        }) : exec;
        this.ep = ep == null ? new EP(exec) : ep;
        this.ep.addListenerSource(this);
        this.sched = sched == null ? new Sched() : sched;
        attrs.putIfAbsent("delimiter", "\n");
        this.aioClient = new AioClient(attrs, exec) {
            @Override
            protected void receive(byte[] bs, AioStream stream) {
                try {
                    receiveReply(new String(bs, _charset.get()), stream);
                } catch (Exception e) {
                    log.error("client receive reply error", e);
                }
            }
        };
        this.aioServer = new AioServer(attrs, exec) {
            @Override
            protected void receive(byte[] bs, AioStream stream) {
                try {
                    receiveMsg(new String(bs, _charset.get()), stream);
                } catch (Exception e) {
                    log.error("server receive msg error", e);
                }
            }
        };
        this.aioServer.start();
    }


    /**
     * stop
     */
    public void stop() {
        aioClient.stop(); aioServer.stop();
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
            nodes.withReadLock(() -> nodes.iterator().forEachRemaining(node -> {
                if (predicate.test(node)) {
                    exec.execute(() -> doSend.accept(node, null));
                }
            }));
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
     * 开始自动心跳
     */
    public void autoHeartbeat() {
        final Supplier<Duration> nextTimeFn =() -> {
            Integer minInterval = getInteger("minInterval", 60);
            Integer randomInterval = getInteger("randomInterval", 180);
            return Duration.ofSeconds(minInterval + new Random().nextInt(randomInterval));
        };
        // 每隔一段时间触发一次心跳, 1~4分钟随机心跳
        final Runnable fn = new Runnable() {
            @Override
            public void run() {
                sync();
                ep.fire("sched.after", nextTimeFn.get(), this);
            }
        };
        fn.run();
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
            doSyncFn.get().run();
            // lastSyncSuccess = System.currentTimeMillis()
        } catch (Exception ex) {
            sched.after(Duration.ofSeconds(getInteger("errorUpInterval", 30) + new Random().nextInt(60)), this::sync);
            log.error("App up error", ex);
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
            JSONObject d = null;
            try { d = msgJo.getJSONObject("data"); appUp(d, stream); }
            catch (Exception ex) {
                log.error("App up error!. data: " + d, ex);
            }
        } else if ("cmd-log".equals(t)) { // telnet 命令行设置日志等级
            // telnet localhost 8001
            // 例: {"type":"cmd-log", "data": "core.module.remote: debug"}
            String[] arr = msgJo.getString("data").split(":");
            // Log.setLevel(arr[0].trim(), arr[1].trim())
            //stream.reply("set log level success".getBytes(_charset.get()));
        } else if (t != null && t.startsWith("ls ")) {
            // {"type":"ls apps"}
            String[] arr = t.split(" ");
            if (arr.length > 0 && "apps".equalsIgnoreCase(arr[1])) { // ls apps
                try {
                    stream.reply(JSON.toJSONString(nodeMap).getBytes(_charset.get()));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            } else if (arr.length > 1 && "app".equalsIgnoreCase(arr[1])) { // ls app gy
                try {
                    stream.reply(JSON.toJSONString(nodeMap.get(arr[2])).getBytes(_charset.get()));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            log.error("Not support exchange data type '{}'", t);
            stream.close();
        }
    }


    /**
     * 接收远程回响的消息
     * @param reply 回应消息
     * @param se AioSession
     */
    protected void receiveReply(final String reply, final AioStream se) {
        log.trace("Receive reply from '{}': {}", se.getRemoteAddress(), reply);
        JSONObject jo = JSON.parseObject(reply, Feature.OrderedField);
        if ("updateAppInfo".equals(jo.getString("type"))) {
            updateAppInfo(jo.getJSONObject("data"));
        }
        else if ("event".equals(jo.getString("type"))) {
            receiveEventResp(jo.getJSONObject("data"));
        }
    }


    /**
     * 接收远程事件返回的数据. 和 {@link #doFire(EC, String, String, List)} 对应
     * @param data
     */
    protected void receiveEventResp(JSONObject data) {
        log.debug("Receive event response: {}", data);
        EC ec = ecMap.remove(data.getString("id"));
        if (ec != null) ec.errMsg(data.getString("exMsg")).result(data.get("result")).resume().tryFinish();
    }


    /**
     * 接收远程事件的执行请求
     * @param data 数据
     * @param se AioSession
     */
    protected void receiveEventReq(final JSONObject data, final AioStream se) {
        log.debug("Receive event request: {}", data);
        boolean fReply = (Boolean.TRUE == data.getBoolean("reply")); // 是否需要响应
        try {
            String eId = data.getString("id");
            String eName = data.getString("name");

            final EC ec = new EC();
            ec.id(eId);
            // 组装方法参数
            ec.args(data.getJSONArray("args") == null ? null : data.getJSONArray("args").stream().map(o -> {
                JSONObject jo = (JSONObject) o;
                String t = jo.getString("type");
                if (jo.isEmpty()) return null; // 参数为null
                else if (String.class.getName().equals(t)) return jo.getString("value");
                else if (Boolean.class.getName().equals(t)) return jo.getBoolean("value");
                else if (Integer.class.getName().equals(t)) return jo.getInteger("value");
                else if (Long.class.getName().equals(t)) return jo.getLong("value");
                else if (Double.class.getName().equals(t)) return jo.getDouble("value");
                else if (Short.class.getName().equals(t)) return jo.getShort("value");
                else if (Float.class.getName().equals(t)) return jo.getFloat("value");
                else if (BigDecimal.class.getName().equals(t)) return jo.getBigDecimal("value");
                else if (JSONObject.class.getName().equals(t) || Map.class.getName().equals(t)) return jo.getJSONObject("value");
                else if (JSONArray.class.getName().equals(t) || List.class.getName().equals(t)) return jo.getJSONArray("value");
                else throw new IllegalArgumentException("Not support parameter type '" + t + "'");
            }).toArray());

            if (fReply) { // 是否需要响应结果给远程的调用方
                JSONObject result = new JSONObject(3);
                byte[] bs = JSON.toJSONString(
                        new JSONObject(3)
                                .fluentPut("type", "event")
                                .fluentPut("source", new JSONObject(2).fluentPut("name", appName).fluentPut("id", appId))
                                .fluentPut("data", result),
                        SerializerFeature.WriteMapNullValue
                ).getBytes(_charset.get());
                ec.completeFn((ec1) -> {
                    result.put("id", ec.id());
                    if (!ec.isSuccess()) { result.put("exMsg", ec.failDesc()); }
                    result.put("result", ec.result);
                    se.reply(bs);
                });
            }
            ep.fire(eName, ec.sync()); // 同步执行, 没必要异步去切换线程
        } catch (Exception ex) {
            log.error("invoke event error. data: " + data, ex);
            if (fReply) {
                JSONObject result = new JSONObject(3);
                result.put("id", data.getString("id"));
                result.put("result", null);
                result.put("exMsg", ex.getMessage() != null ? ex.getMessage(): ex.getClass().getName());
                JSONObject res = new JSONObject(3)
                        .fluentPut("type", "event")
                        .fluentPut("source", new JSONObject(2).fluentPut("name", appName).fluentPut("id", appId))
                        .fluentPut("data", result);
                try {
                    se.reply(JSON.toJSONString(res, SerializerFeature.WriteMapNullValue).getBytes(_charset.get()));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }


    /**
     * client -> master
     * 接收集群应用的数据上传, 应用上线通知
     * NOTE: 此方法线程已安全
     * id 和 tcp 必须唯一
     * @param data 节点应用上传的数据 {"name": "应用名", "id": "应用实例id", "tcp":"host:port", "http":"host:port", "udp": "host:port"}
     * @param se {@link AioStream}
     */
    protected void appUp(final Map data, final AioStream se) {
        if (data == null || data.get("name") == null || data.get("tcp") == null || data.get("id") == null) { // 数据验证
            log.warn("Node up data incomplete: " + data);
            return;
        }
        if (appId.equals(data.get("id").toString()) && se != null) return; // 不接收本应用的数据
        data.put("_uptime", System.currentTimeMillis());
        log.debug("Receive node up: {}", data);

        //1. 先删除之前的数据,再添加新的
        AtomicBoolean isNew = new AtomicBoolean(true);
        final SafeList<Node> apps = nodeMap.computeIfAbsent(data.get("name").toString(), (s) -> new SafeList<>());
        apps.withReadLock(() -> {
            for (Iterator<Node> itt = apps.iterator(); itt.hasNext(); ) {
                Node node = itt.next();
                if (node.id.equals(data.get("id"))) { // 更新
                    node.tcp = (String) data.get("tcp"); node.http = (String) data.get("http"); node.udp = (String) data.get("udp"); node.master = (Boolean) data.get("master"); node._uptime = (Long) data.get("_uptime");
                    isNew.set(false); break;
                }
            }
        });
        if (isNew.get()) { // 新增
            Node node = new Node();
            node.id = (String) data.get("id");
            node.name = (String) data.get("name");
            node.tcp = (String) data.get("tcp");
            node.http = (String) data.get("http");
            node.master = (Boolean) data.get("master");
            node._uptime = (Long) data.get("_uptime");
            apps.add(node);
            log.info("New node online. {}", node);
        }

        //2. 遍历所有的数据, 删除不必要的数据, 同步注册信息
        for (Iterator<Map.Entry<String, SafeList<Node>>> itt = nodeMap.entrySet().iterator(); itt.hasNext(); ) {
            Map.Entry<String, SafeList<Node>> e = itt.next();
            e.getValue().withWriteLock(() -> {
                for (Iterator<Node> itt2 = e.getValue().iterator(); itt2.hasNext(); ) {
                    final Node node = itt2.next();
                    // 删除空的坏数据
                    if (node == null) {itt2.remove(); continue;}
                    // 删除和当前up的节点相同的tcp的节点(节点重启,但节点上次的信息还没被移除)
                    if (node.id.equals(data.get("id")) && node.tcp.equals(data.get("tcp"))) {
                        itt2.remove();
                        log.info("Drop same tcp expire node: {}", node);
                        continue;
                    }
                    // 删除一段时间未活动的注册信息, dropAppTimeout 单位: 分钟
                    if ((System.currentTimeMillis() - node._uptime > getInteger("dropAppTimeout", 15) * 60 * 1000) && appId.equals(node.id)) {
                        itt2.remove();
                        log.warn("Drop timeout node: {}", node);
                        continue;
                    }
                    // 更新当前App up的时间
                    if (node.id.equals(appId) && (System.currentTimeMillis() - node._uptime > 1000 * 60)) { // 超过1分钟更新一次,不必每次更新
                        node._uptime = System.currentTimeMillis();
                        Map<String, Object> self = getAppInfo(); // 判断当前机器ip是否变化
                        if (self != null && !node.tcp.equals(self.get("tcp"))) {
                            node.tcp = (String) self.get("tcp"); node.http = (String) self.get("http"); node.udp = (String) self.get("udp"); node.master = (Boolean) self.get("master");
                        }
                    }
                    // 返回所有的注册信息给当前来注册的客户端应用
                    if (node.id.equals(data.get("id")) && se != null) {
                        try {
                            se.reply(new JSONObject(2).fluentPut("type", "updateAppInfo").fluentPut("data", JSON.toJSON(node)).toString().getBytes(_charset.get()));
                        } catch (UnsupportedEncodingException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
            });
            // 删除没有对应的服务信息的应用
            if (e.getValue().isEmpty()) {itt.remove(); continue;}
            // 如果是新系统上线, 则主动通知其它系统
            if (isNew.get() && appId.equals(data.get("id")) && e.getValue().size() > 0) {
                ep.fire("remote", EC.of(this).async(true).attr("toAll", true).args(e.getKey(), "updateAppInfo", Arrays.asList(data)));
            }
        }
    }


    /**
     * master -> client
     * 更新 app 信息
     * @param data app node信息
     *  例: {"name":"rc", "id":"rc_b70d18d52269451291ea6380325e2a84", "tcp":"192.168.56.1:8001","http":"localhost:8000"}
     *  属性不为空: name, id, tcp
     */
    @EL(name = "updateAppInfo", async = false)
    protected void updateAppInfo(final JSONObject data) {
        if (data == null || data.get("name") == null || data.get("tcp") == null || data.get("id") == null) { // 数据验证
            log.warn("App up data incomplete: " + data);
            return;
        }
        Map<String, Object> appInfo = getAppInfo();
        if (appId.equals(data.getString("id")) || (appInfo != null && Objects.equals(appInfo.get("tcp"), data.getString("tcp")))) return; // 不把系统本身的信息放进去
        log.trace("Update app info: {}", data);

        SafeList<Node> apps = nodeMap.computeIfAbsent(data.getString("name"), s -> new SafeList<>());
        apps.withWriteLock(() -> {
            boolean add = true;
            for (Iterator<Node> itt = apps.iterator(); itt.hasNext(); ) {
                final Node node = itt.next();
                if (node.id.equals(data.getString("id"))) { // 更新
                    add = false;
                    if (data.getLong("_uptime") > node._uptime) {
                        node.tcp = data.getString("tcp"); node.http = data.getString("http"); node.udp = data.getString("udp"); node.master = data.getBoolean("master"); node._uptime = data.getLong("_uptime");
                        log.trace("Update node info: {}", node);
                    }
                    break;
                } else if (node.tcp.equals(data.getString("tcp"))) {
                    if (data.getLong("_uptime") > node._uptime) { // 删除 tcp 相同, id 不同, 已过期的节点
                        itt.remove();
                        log.info("Drop same tcp expire node: {}", node);
                    } else add = false;
                    break;
                } else if (System.currentTimeMillis() - node._uptime > getInteger("dropAppTimeout", 15) * 60 * 1000 && node.id != appId) {
                    itt.remove(); // 删除过期不活动的的节点
                    log.warn("Drop timeout node: {}", node);
                }
            }
            if (add) {
                Node node = new Node();
                node.id = (String) data.get("id");
                node.name = (String) data.get("name");
                node.tcp = (String) data.get("tcp");
                node.http = (String) data.get("http");
                node.master = (Boolean) data.get("master");
                node._uptime = (Long) data.get("_uptime");
                apps.add(node);
                log.info("New node added. '{}'", node);
            }
        });
    }


    /**
     * host:port
     */
    class Hp {
        String host; Integer port;
        public Hp(String host, Integer port) {
            this.host = host;this.port = port;
        }
    }

    /**
     * 远程数据同步函数
     */
    final LazySupplier<Runnable> doSyncFn = new LazySupplier<>(() -> new Runnable() {
        // 数据上传的应用集(host1:port1,host2:port2 ...)
        final List<Hp> hps = _masterHps.get() == null ? null : Arrays.stream(_masterHps.get().split(",")).map(hp -> {
            if (hp == null) return null;
            try {
                String[] arr = hp.split(":");
                String host = arr[0].trim();
                return new Hp(host.isEmpty() ? "127.0.0.1" : host, Integer.valueOf(arr[1].trim()));
            } catch (Exception ex) {
                log.error("'masterHps' config error. " + hp, ex);
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());

        // 本应用的hps(多个host:port)
        final Set<String> selfHps = ((Supplier<Set<String>>) () -> {
            Set<String> hps = new HashSet<>();
            String hpCfg = getStr("hp", "");
            String[] arr = hpCfg.split(":");
            if (arr[0] != null && !arr[0].isEmpty()) { // 如果指定了绑定ip
                hps.add(hpCfg);
            } else { // 如果没指定绑定ip,则把本机所有ip
                hps.add("localhost:" + arr[1]);
                hps.add("127.0.0.1:" + arr[1]);
                hps.add(AioServer.ipv4() + ":" + arr[1]);
            }
            String exposeTcp = getStr("exposeTcp", null);
            if (exposeTcp != null && !exposeTcp.isEmpty()) hps.add(exposeTcp);
            return hps;
        }).get();

        /**
         * 上传本应用数据到集群
         * @param data
         */
        void upToHps(String data) throws Exception {
            // 如果是域名,得到所有Ip, 除本机上的本应用
            List<Hp> ls = hps.stream().flatMap(hp -> {
                    try {
                        return Arrays.stream(InetAddress.getAllByName(hp.host))
                                .map(addr -> {
                                    if (selfHps.contains(addr.getHostAddress()+ ":" +hp.port)) return null;
                                        else return new Hp(addr.getHostAddress(), hp.port);
                                    });
                    } catch (UnknownHostException e) {
                        log.error("", e);
                    }
                return null;
            }).collect(Collectors.toList());
            if (ls.isEmpty()) return;

            if (_master.get()) { // 如果是master, 则同步所有
                for (Hp hp : ls) {
                    aioClient.send(hp.host, hp.port, data.getBytes(_charset.get()));
                }
                log.debug("App up success. {}", data);
            } else {
                Hp hp = ls.get(new Random().nextInt(ls.size()));
                aioClient.send(hp.host, hp.port, data.getBytes(_charset.get()), ex -> {
                    log.warn("App up fail. " + data, ex);
                }, (se) -> {
                    log.debug("App up success. {}", data);
                });
            }
        }

        @Override
        public void run() {
            if (hps.isEmpty()) {
                log.warn("Not found available master app, please check config 'masterHps'");
                return;
            }
            try {
                String data = ((Supplier<String>) () -> { // 构建上传的数据格式
                    Map<String, Object> info = getAppInfo();
                    if (info == null || info.isEmpty()) return null;
                    return new JSONObject(3).fluentPut("type", "appUp")
                            .fluentPut("source", new JSONObject(2).fluentPut("name", appName).fluentPut("id", appId))
                            .fluentPut("data", info)
                            .toString();
                }).get();
                if (data == null) return;
                upToHps(data); //数据上传
                dataVersionMap.forEach((s, dataVersion) -> dataVersion.sync()); //集群版本数据同步
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    });


    /**
     * 应用自己的信息
     * 例: {"id":"gy_GRLD5JhT4g", "name":"rc", "tcp":"192.168.2.104:8001", "http":"192.168.2.104:8000", "master": true}
     */
    public Map<String, Object> getAppInfo() {
        Map<String, Object> info = new LinkedHashMap(5);
        info.put("id", appId);
        info.put("name", appName);
        // http
        info.put("http", getStr("exposeHttp", (String) ep.fire("http.hp")));
        // tcp
        info.put("tcp", getStr("exposeTcp", aioServer.getHp()));
        info.put("master", _master.get());
        return info;
    }


    /**
     * getter {@link AioClient}
     * @return
     */
    public AioClient getAioClient() { return aioClient; }


    /**
     * getter {@link AioServer}
     * @return
     */
    public AioServer getAioServer() { return aioServer; }
}
