package xqa.integration;


import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import xqa.IngestBalancer;
import xqa.commons.MessageSender;

import javax.jms.DeliveryMode;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IngestBalancerTest {
    @Test
    void ingestBalancerShowUsage() {
        assertThrows(IngestBalancer.CommandLineException.class,
                () -> {
                    IngestBalancer ingest = new IngestBalancer();
                    ingest.consumeCommandLine(new String[]{});
                });
    }

    @Test
    void singleIngest() throws Exception {
        IngestBalancer ingestBalancer = new IngestBalancer();
        ingestBalancer.consumeCommandLine(new String[] {"-message_broker_host", "127.0.0.1", "-pool_size", "3"});
        ingestBalancer.start();

        MockShard mockShard = new MockShard();
        mockShard.start();

        MessageSender messageSender = new MessageSender(ingestBalancer.messageBrokerHost);
        messageSender.sendMessage(MessageSender.DestinationType.Queue, "xqa.ingest", UUID.randomUUID().toString(), null, "/a/b/c.xml", xmlFileContents(), DeliveryMode.PERSISTENT);

        while (mockShard.digestOfMostRecentMessage == null) {
            Thread.sleep(1000);
        }

        assertEquals("192a0c3918e308c1374d57256b183045393c1cf9053a8614e9d7bb24b8261358", mockShard.digestOfMostRecentMessage);

        messageSender.sendMessage(MessageSender.DestinationType.Topic, "xqa.cmd.stop", UUID.randomUUID().toString(), null, null, null, DeliveryMode.PERSISTENT);
        messageSender.close();
        mockShard.join();
        ingestBalancer.join();
    }

    private String xmlFileContents() throws IOException {
        URL url = getClass().getResource("/test-data/nicn_nwp_078_17101111_0195.xml");
        return FileUtils.readFileToString(new File(url.getPath()), "UTF-8");
    }
}
