package xqa;

import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFactory;
import org.junit.jupiter.api.Test;
import xqa.commons.qpid.jms.MessageMaker;

import javax.jms.BytesMessage;
import javax.jms.Message;
import java.util.List;
import java.util.Vector;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

class SmallestShardTest {
    @Test
    void findSmallestShard() throws Exception {

        IngestBalancer ingestBalancer = new IngestBalancer();
        ingestBalancer.processCommandLine(new String[]{"-message_broker_host", "127.0.0.1"});

        InserterThread inserterThread = new InserterThread(
                ingestBalancer.serviceId,
                ingestBalancer.messageBroker,
                mock(BytesMessage.class),
                ingestBalancer.poolSize,
                ingestBalancer.destinationEvent,
                ingestBalancer.destinationShardSize);

        List<Message> shardSizeResponses = new Vector<>();

        JmsMessageFactory factory = new JmsTestMessageFactory();

        JmsBytesMessage big = factory.createBytesMessage();
        big.writeBytes("10".getBytes());
        shardSizeResponses.add(big);

        JmsBytesMessage bigger = factory.createBytesMessage();
        bigger.writeBytes("30".getBytes());
        shardSizeResponses.add(bigger);

        JmsBytesMessage smallest = factory.createBytesMessage();
        smallest.writeBytes("5".getBytes());
        shardSizeResponses.add(smallest);

        assertEquals(
                MessageMaker.getBody(smallest),
                MessageMaker.getBody(inserterThread.findSmallestShard(shardSizeResponses)));
    }
}
