package xqa;

import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFactory;
import org.junit.jupiter.api.Test;
import xqa.commons.MessageLogging;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class SmallestShardTest {
    @Test
    public void findSmallestShard() throws UnsupportedEncodingException, JMSException {
        InserterThread inserter = new InserterThread(mock(BytesMessage.class));

        JmsMessageFactory factory = new JmsTestMessageFactory();

        JmsBytesMessage big = factory.createBytesMessage();
        big.writeBytes("10".getBytes());
        inserter.shardSizeResponses.add(big);

        JmsBytesMessage bigger = factory.createBytesMessage();
        bigger.writeBytes("30".getBytes());
        inserter.shardSizeResponses.add(bigger);

        JmsBytesMessage smallest = factory.createBytesMessage();
        smallest.writeBytes("5".getBytes());
        inserter.shardSizeResponses.add(smallest);

        assertEquals(MessageLogging.getTextFromMessage(smallest), MessageLogging.getTextFromMessage(inserter.smallestShard()));
    }
}
