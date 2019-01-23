package xqa.integration;


import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.UUID;

import javax.jms.JMSException;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.github.jameshnsears.configuration.ConfigurationAccessor;
import com.github.jameshnsears.configuration.ConfigurationParameterResolver;
import com.github.jameshnsears.docker.DockerClient;

import xqa.IngestBalancer;
import xqa.commons.qpid.jms.MessageBroker;
import xqa.commons.qpid.jms.MessageMaker;

@ExtendWith(ConfigurationParameterResolver.class)
class IngestBalancerTest {
    private DockerClient dockerClient;

    @BeforeEach
    public void startMessageBroker(final ConfigurationAccessor configurationAccessor)
            throws IOException {
        dockerClient = new DockerClient();
        dockerClient.pull(configurationAccessor.images());
        dockerClient.startContainers(configurationAccessor);
    }

    @AfterEach
    public void stopMessageBroker(final ConfigurationAccessor configurationAccessor)
            throws IOException {
        dockerClient.rmContainers(configurationAccessor);
    }

    @Test
    void singleIngest() throws Exception {
        final IngestBalancer ingestBalancer = new IngestBalancer();
        ingestBalancer.processCommandLine(new String[]{
                "-message_broker_host",
                "0.0.0.0",
                "-pool_size",
                "3",
                "-insert_thread_wait",
                "10000"});
        ingestBalancer.start();

        final MockShard mockShard = new MockShard();
        mockShard.start();

        final MessageBroker messageBroker = new MessageBroker(
                "0.0.0.0",
                5672,
                "admin",
                "admin",
                10);

        sendIngestMessage(ingestBalancer.destinationIngest, messageBroker);

        while (mockShard.digestOfMostRecentMessage == null) {
            Thread.sleep(1000);
        }

        assertEquals(
                "192a0c3918e308c1374d57256b183045393c1cf9053a8614e9d7bb24b8261358",
                mockShard.digestOfMostRecentMessage,
                "digest does not match");

        sendStopMessage(ingestBalancer.destinationCmdStop, messageBroker);

        mockShard.join();
        ingestBalancer.join();

        messageBroker.close();
    }

    @Test
    void noResponseFromShard() throws Exception {
        final IngestBalancer ingestBalancer = new IngestBalancer();
        ingestBalancer.processCommandLine(new String[]{
                "-message_broker_host",
                "0.0.0.0",
                "-pool_size",
                "3",
                "-insert_thread_wait",
                "500"});
        ingestBalancer.start();

        final MessageBroker messageBroker = new MessageBroker(
                "0.0.0.0",
                5672,
                "admin",
                "admin",
                10);

        sendIngestMessage(ingestBalancer.destinationIngest, messageBroker);

        Thread.sleep(2000);

        sendStopMessage(ingestBalancer.destinationCmdStop, messageBroker);

        ingestBalancer.join();

        messageBroker.close();
    }

    private void sendIngestMessage(final String destination,
                                   final MessageBroker messageBroker)
                                           throws JMSException, IOException, MessageBroker.MessageBrokerException {
        messageBroker.sendMessage(MessageMaker.createMessage(
                messageBroker.getSession(),
                messageBroker.getSession().createQueue(destination),
                UUID.randomUUID().toString(),
                xmlFilePath(),
                xmlFileContents()));
    }

    private void sendStopMessage(final String destination,
                                 final MessageBroker messageBroker)
                                         throws JMSException, UnsupportedEncodingException, MessageBroker.MessageBrokerException {
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
