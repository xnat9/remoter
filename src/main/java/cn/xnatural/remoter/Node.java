package cn.xnatural.remoter;

import java.io.Serializable;

/**
 * 集群节点 信息
 */
class Node implements Serializable {
    /**
     * 节点id, 对应 app.id
     */
    public String id;
    /**
     * 节点名, 对应 app.name
     */
    public String name;
    /**
     * tcp endpoint -> host:port
     */
    public String tcp;
    /**
     * http endpoint -> host:port
     */
    public String http;
    /**
     * udp endpoint -> host:port
     */
    public String udp;
    /**
     * 是否为 master {@link Remoter#_master}
     */
    public Boolean master;
    /**
     * 上传数据的时间. app up 时间
     */
    public Long _uptime;


    @Override
    public String toString() {
        return "Node@" + Integer.toHexString(hashCode()) +
                "{id=" + id +
                ", name=" + name +
                ", tcp=" + tcp +
                ", http=" + http +
                ", udp=" + udp +
                ", master=" + master +
                ", _uptime=" + _uptime +
                '}';
    }
}
