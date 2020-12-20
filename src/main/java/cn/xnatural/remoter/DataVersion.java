package cn.xnatural.remoter;

import cn.xnatural.enet.event.EC;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static cn.xnatural.remoter.Remoter.log;

/**
 * 数据版本同步器, 同步集群数据
 * 多个系统之前的数据同步
 * NOTE: 最终一致性. 场景1: 内存数据和数据库数据同步
 */
public class DataVersion {
    // 数据集key
    protected   final    String              key;
    // 数据key -> (版本, 最开始同步时间, 发送)
    protected final Map<String, Record> records = new ConcurrentHashMap<>();
    final Remoter remoter;


    DataVersion(Remoter remoter, String key) {
        this.remoter = remoter;
        this.key = key;
    }


    /**
     * 数据记录
     * 集群中数据同步的数据记录
     */
    class Record {
        //数据版本
        Long    version;
        //上次发送时间
        Long    lastSendTime;
        //状态: 发送 or 接收
        Boolean send;
        //具体数据
        Object  data;
    }

    /**
     * 更新数据到集群
     * @param dataKey 数据key
     * @param version 数据更新版本. NOTE: 每次更新得保证比之前的版本大
     * @param data 数据内容 支持基本类型, String NOTE: 建议数据持久化
     * @return {@link DataVersion}
     */
    public DataVersion update(String dataKey, Long version, Object data) {
        if (dataKey == null || dataKey.isEmpty()) throw new IllegalArgumentException("update dataVersion dataKey must not be empty");
        if (version == null) throw new IllegalArgumentException("update dataVersion version must not be null");
        Record record = records.computeIfAbsent(dataKey, (k) -> new Record());
        if (record.version == null || record.version < version) { // 有新版本数据更新
            record.version = version;
            record.data = data;
            record.lastSendTime = System.currentTimeMillis();
            record.send = true;
            log.debug("Update DataVersion: key: " + key + ", dataKey: " + dataKey + ", version: " + version + ", data: " + data);
            remoter.ep.fire("remote", EC.of(this).attr("toAll", true).args(remoter.appName, "dataVersion", Arrays.asList(key, dataKey, version, data)));
        }
        return this;
    }



    /**
     * 接收远程数据更新
     * @param dataKey 数据key
     * @param version 数据更新版本
     * @param data 数据
     * @return
     */
    DataVersion receive(String dataKey, Long version, Object data) {
        if (dataKey == null || dataKey.isEmpty()) throw new IllegalArgumentException("receive dataVersion dataKey must not be empty");
        if (version == null) throw new IllegalArgumentException("receive dataVersion version must not be null");
        Record record = records.computeIfAbsent(dataKey, (k) -> new Record());
        if (record.version == null || record.version < version) {
            record.version = version;
            record.send = false;
            record.data = data;
            log.debug("Receive DataVersion: key: " + key + ", dataKey: " + dataKey + ", version: " + version + ", data: " + data);
            remoter.ep.fire(key + ".dataVersion", dataKey, version, data);
        }
        return this;
    }


    /**
     * 数据同步
     */
    void sync() {
        long interval = Duration.ofMinutes(Long.valueOf(remoter.attrs.getOrDefault("dataVersion.dropTimeout", 120L).toString())).toMillis();
        records.forEach((key, record) -> {
            if (record.send && record.lastSendTime != null && System.currentTimeMillis() - record.lastSendTime <= interval) {
                remoter.ep.fire("remote", EC.of(this).attr("toAll", true).args(remoter.appName, "dataVersion", Arrays.asList(this.key, key, record.version, record.data)));
                log.trace("Sync DataVersion: key: " + this.key + ", dataKey: " + key + ", version: " + record.version + ", data: " + record.data);
            }
        });
    }
}
