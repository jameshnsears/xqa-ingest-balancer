package xqa.integration;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xqa.commons.qpid.jms.MessageBroker;
import xqa.commons.qpid.jms.MessageLogger;
import xqa.commons.qpid.jms.MessageMaker;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import java.util.UUID;

class MockShard extends Thread implements Runnable, MessageListener {
    private static final Logger logger = LoggerFactory.getLogger(MockShard.class);
    public String digestOfMostRecentMessage;
    private MessageBroker messageBroker;
    private boolean stop = false;
    private final String destinationInsertRoot = "xqa.shard.insert.";
    private final String destinationShardSize = "xqa.shard.size";
    private final String destinationCmdStop = "xqa.cmd.stop";
    private Destination insertUuidDestination;

    public MockShard() throws MessageBroker.MessageBrokerException {
        messageBroker = new MessageBroker("0.0.0.0", 5672, "admin", "admin", 3);
        setName("MockShard");
    }

    public void run() {
        registerListeners();

        while (!stop) {
            try {
                Thread.sleep(500);
            } catch (Exception exception) {
                logger.error(exception.getMessage());
                exception.printStackTrace();
                System.exit(1);
            }
        }

        try {
            messageBroker.close();
        } catch (Exception exception) {
            logger.error(exception.getMessage());
            exception.printStackTrace();
            System.exit(1);
        }
    }

    private void registerListeners() {
        try {
            Destination cmdStopDestination = messageBroker.getSession().createTopic(destinationCmdStop);
            MessageConsumer cmdStopConsumer = messageBroker.getSession().createConsumer(cmdStopDestination);
            cmdStopConsumer.setMessageListener(this);

            Destination sizeDestination = messageBroker.getSession().createTopic(destinationShardSize);
            MessageConsumer sizeConsumer = messageBroker.getSession().createConsumer(sizeDestination);
            sizeConsumer.setMessageListener(this);

            synchronized (this) {
                insertUuidDestination = messageBroker.getSession().createQueue(destinationInsertRoot.concat(
                        UUID.randomUUID().toString().split("-")[0]));
                MessageConsumer insertUuidConsumer = messageBroker.getSession().createConsumer(insertUuidDestination);
                insertUuidConsumer.setMessageListener(this);
            }
        } catch (Exception exception) {
            logger.error(exception.getMessage());
            exception.printStackTrace();
            System.exit(1);
        }
    }

    public void onMessage(Message message) {
        try {
            switch (message.getJMSDestination().toString()) {
                case destinationCmdStop:
                    logger.debug(MessageLogger.log(MessageLogger.Direction.RECEIVE, message, false));
                    stop = true;
                    break;
                case destinationShardSize:
                    logger.debug(MessageLogger.log(MessageLogger.Direction.RECEIVE, message, false));
                    sendSizeReply(message);
                    break;
                default:
                    logger.debug(MessageLogger.log(MessageLogger.Direction.RECEIVE, message, true));
                    synchronized (this) {
                        digestOfMostRecentMessage = DigestUtils.sha256Hex(MessageMaker.getBody(message));
                    }
                    break;
            }
        } catch (Exception exception) {
            logger.error(exception.getMessage());
            exception.printStackTrace();
            System.exit(1);
        }
    }

    private void sendSizeReply(Message jmsBytesMessage) throws Exception {
        Message message = MessageMaker.createMessage(
                messageBroker.getSession(),
                jmsBytesMessage.getJMSReplyTo(),
                this.insertUuidDestination,
                jmsBytesMessage.getJMSCorrelationID(),
                "0");

        messageBroker.sendMessage(message);
    }
}
