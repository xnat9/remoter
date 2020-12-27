import cn.xnatural.remoter.Remoter;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RemoterTest {

    final static Logger log = LoggerFactory.getLogger(RemoterTest.class);


    @Test
    void server() throws Exception {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("hp", ":8001");
        attrs.put("master", true);
        // attrs.put("masterHps", "xnatural.cn:8001");
        Remoter remoter = new Remoter("server", "s1", attrs).autoHeartbeat();
        //log.info(remoter.fire("gy", "eName7", Arrays.asList("p1")).toString());
        Thread.sleep(1000L * 60 * 10);
    }


    @Test
    void client() throws Exception {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("masterHps", ":8001");
        attrs.put("hp", ":4001");
        Remoter remoter = new Remoter("client", "c1", attrs).autoHeartbeat();
        Thread.sleep(1000L * 60 * 10);
        //log.info(remoter.fire("gy", "eName7", Arrays.asList("p1")).toString());
    }
}
