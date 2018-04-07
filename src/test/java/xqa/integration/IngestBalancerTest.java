package xqa.integration;


import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import xqa.IngestBalancer;
import xqa.commons.qpid.jms.MessageBroker;
import xqa.commons.qpid.jms.MessageMaker;

import javax.jms.JMSException;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IngestBalancerTest {
    @Test
    void ingestBalancerShowUsage() {
        assertThrows(IngestBalancer.CommandLineException.class,
                () -> IngestBalancer.main(new String[]{}));
    }

    @Test
    void singleIngest() throws Exception {
        IngestBalancer ingestBalancer = new IngestBalancer();
        String messageBrokerHost = "0.0.0.0";
        ingestBalancer.processCommandLine(new String[]{
                "-message_broker_host", messageBrokerHost,
                "-pool_size", "3",
                "-insert_thread_wait", "10000"});
        ingestBalancer.start();

        MockShard mockShard = new MockShard();
        mockShard.start();

        MessageBroker messageBroker = new MessageBroker(messageBrokerHost, 5672, "admin", "admin", 3);

        sendIngestMessage(ingestBalancer.destinationIngest, messageBroker);

        while (mockShard.digestOfMostRecentMessage == null) {
            Thread.sleep(1000);
        }

        assertEquals("192a0c3918e308c1374d57256b183045393c1cf9053a8614e9d7bb24b8261358", mockShard.digestOfMostRecentMessage);

        sendStopMessage(ingestBalancer.destinationCmdStop, messageBroker);

        mockShard.join();
        ingestBalancer.join();

        messageBroker.close();
    }

    private void sendIngestMessage(String destination,
                                   MessageBroker messageBroker) throws JMSException, IOException {
        messageBroker.sendMessage(MessageMaker.createMessage(
                messageBroker.getSession(),
                messageBroker.getSession().createQueue(destination),
                UUID.randomUUID().toString(),
                xmlFilePath(),
                xmlFileContents()));
    }

    private void sendStopMessage(String destination,
                                 MessageBroker messageBroker) throws JMSException, UnsupportedEncodingException {
        messageBroker.sendMessage(MessageMaker.createMessage(
                messageBroker.getSession(),
                messageBroker.getSession().createTopic(destination),
                UUID.randomUUID().toString(),
                ""));
    }

    private String xmlFileContents() throws IOException {
        return FileUtils.readFileToString(
                new File(xmlFilePath()),
                "UTF-8");
    }

    private String xmlFilePath() {
        return getClass().getResource("/test-data/nicn_nwp_078_17101111_0195.xml").getPath();
    }
}
