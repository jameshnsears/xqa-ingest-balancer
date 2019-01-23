package xqa.integration;

import java.util.UUID;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xqa.commons.qpid.jms.MessageBroker;
import xqa.commons.qpid.jms.MessageLogger;
import xqa.commons.qpid.jms.MessageMaker;

class MockShard extends Thread implements Runnable, MessageListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(MockShard.class);
    private final String destinationInsertRoot = "xqa.shard.insert.";
    private final String destinationShardSize = "xqa.shard.size";
    private final String destinationCmdStop = "xqa.cmd.stop";
    public String digestOfMostRecentMessage;
    private MessageBroker messageBroker;
    private boolean stop;
    private Destination insertUuidDestination;

    public MockShard() throws MessageBroker.MessageBrokerException, InterruptedException {
        messageBroker = new MessageBroker("0.0.0.0", 5672, "admin", "admin", 10);
        setName("MockShard");
    }

    public void run() {
        registerListeners();

        while (!stop) {
            try {
                Thread.sleep(500);
            } catch (Exception exception) {
                LOGGER.error(exception.getMessage());
            }
        }

        try {
            messageBroker.close();
        } catch (Exception exception) {
            LOGGER.error(exception.getMessage());
        }
    }

    private void registerListeners() {
        try {
            final Destination cmdStopDestination = messageBroker.getSession().createTopic(destinationCmdStop);
            final MessageConsumer cmdStopConsumer = messageBroker.getSession().createConsumer(cmdStopDestination);
            cmdStopConsumer.setMessageListener(this);

            final Destination sizeDestination = messageBroker.getSession().createTopic(destinationShardSize);
            final MessageConsumer sizeConsumer = messageBroker.getSession().createConsumer(sizeDestination);
            sizeConsumer.setMessageListener(this);

            synchronized (this) {
                insertUuidDestination = messageBroker.getSession().createQueue(destinationInsertRoot.concat(
                        UUID.randomUUID().toString().split("-")[0]));
                MessageConsumer insertUuidConsumer = messageBroker.getSession().createConsumer(insertUuidDestination);
                insertUuidConsumer.setMessageListener(this);
            }
        } catch (Exception exception) {
            LOGGER.error(exception.getMessage());
        }
    }

    public void onMessage(final Message message) {
        try {
            switch (message.getJMSDestination().toString()) {
                case destinationCmdStop:
                    LOGGER.debug(MessageLogger.log(MessageLogger.Direction.RECEIVE, message, false));
                    stop = true;
                    break;
                case destinationShardSize:
                    LOGGER.debug(MessageLogger.log(MessageLogger.Direction.RECEIVE, message, false));
                    sendSizeReply(message);
                    break;
                default:
                    LOGGER.debug(MessageLogger.log(MessageLogger.Direction.RECEIVE, message, true));
                    synchronized (this) {
                        digestOfMostRecentMessage = DigestUtils.sha256Hex(MessageMaker.getBody(message));
                    }
                    break;
            }
        } catch (Exception exception) {
            LOGGER.error(exception.getMessage());
        }
    }

    private void sendSizeReply(final Message message) throws Exception {
        messageBroker.sendMessage(MessageMaker.createMessage(
                messageBroker.getSession(),
                message.getJMSReplyTo(),
                this.insertUuidDestination,
                message.getJMSCorrelationID(),
                "0"));
    }
}
