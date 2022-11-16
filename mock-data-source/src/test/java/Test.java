import com.pgman.goku.util.KafkaUtils;
import com.pgman.goku.util.MockDataUtils;
import com.pgman.goku.util.ObjectUtils;

public class Test {

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        MockDataUtils.mockOrderStreamData(false,5000,3);
    }

    /**
     * kafka-console-consumer.sh --zookeeper localhost:2181 --topic test_topic --from-beginning
     */
    @org.junit.Test
    public void test01() throws Exception{

        while (true) {
            int i=0;
            Thread.sleep(1000);
            KafkaUtils.getInstance().send("test_topic", "test message1" + i++);
        }
    }

}
