import cn.xnatural.remoter.Remoter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RemoterTest {

    final static Logger log = LoggerFactory.getLogger(RemoterTest.class);


    public static void main(String[] args) throws Exception {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("masterHps", "xnatural.cn:8001");
        Remoter remoter = new Remoter("test", "test1", attrs).autoHeartbeat();
        Thread.sleep(1000L);
        log.info(remoter.fire("gy", "eName1", Arrays.asList("p1")).toString());
    }
}
