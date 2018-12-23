package xqa;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Message;

import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFactory;
import org.junit.jupiter.api.Test;

import xqa.commons.qpid.jms.MessageMaker;

class SmallestShardTest {
    @Test
    void findSmallestShard() throws Exception {
        final IngestBalancer ingestBalancer = new IngestBalancer();
        ingestBalancer.processCommandLine(new String[]{"-message_broker_host", "127.0.0.1"});

        final InserterThread inserterThread = new InserterThread(ingestBalancer, mock(BytesMessage.class));

        final List<Message> shardSizeResponses = new ArrayList<>();

        final JmsMessageFactory factory = new JmsTestMessageFactory();

        final JmsBytesMessage big = factory.createBytesMessage();
        big.writeBytes("10".getBytes());
        shardSizeResponses.add(big);

        final JmsBytesMessage smallest = factory.createBytesMessage();
        smallest.writeBytes("5".getBytes());
        shardSizeResponses.add(smallest);

        final JmsBytesMessage bigger = factory.createBytesMessage();
        bigger.writeBytes("30".getBytes());
        shardSizeResponses.add(bigger);

        assertEquals(
                "unable to find smallest shard size reponse",
                MessageMaker.getBody(smallest),
                MessageMaker.getBody(inserterThread.findSmallestShard(shardSizeResponses)));
    }
}
