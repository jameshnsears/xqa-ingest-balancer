package xqa.integration;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.qpid.jms.message.JmsBytesMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xqa.commons.IngestBalancerConnectionFactory;
import xqa.commons.MessageSender;
import xqa.commons.qpid.jms.MessageBroker;
import xqa.commons.qpid.jms.MessageLogger;
import xqa.commons.qpid.jms.MessageMaker;

import javax.jms.*;
import java.util.UUID;

class MockShard extends Thread implements Runnable, MessageListener {
    private static final Logger logger = LoggerFactory.getLogger(MockShard.class);
    public String digestOfMostRecentMessage;
    private boolean stop = false;
    private Destination insertUuidDestination;

    MessageBroker messageBroker;

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

            Destination cmdStopDestination = messageBroker.getSession().createTopic("xqa.cmd.stop");
            MessageConsumer cmdStopConsumer = messageBroker.getSession().createConsumer(cmdStopDestination);
            cmdStopConsumer.setMessageListener(this);

            Destination sizeDestination = messageBroker.getSession().createTopic("xqa.shard.size");
            MessageConsumer sizeConsumer = messageBroker.getSession().createConsumer(sizeDestination);
            sizeConsumer.setMessageListener(this);

            String uuid = UUID.randomUUID().toString().split("-")[0];
            synchronized (this) {
                insertUuidDestination = messageBroker.getSession().createQueue("xqa.shard.insert.".concat(uuid));
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
            if (message.getJMSDestination().toString().equals("xqa.cmd.stop")) {
                logger.debug(MessageLogger.log(MessageLogger.Direction.RECEIVE, message, false));
                stop = true;
            }
            else if (message.getJMSDestination().toString().equals("xqa.shard.size")) {
                logger.debug(MessageLogger.log(MessageLogger.Direction.RECEIVE, message, false));
                sendSizeReply(message);
            }
            else {
                logger.debug(MessageLogger.log(MessageLogger.Direction.RECEIVE, message, true));
                synchronized (this) {
                    digestOfMostRecentMessage = DigestUtils.sha256Hex(MessageMaker.getBody(message));
                }
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
