package xqa.integration;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.qpid.jms.message.JmsBytesMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xqa.commons.MessageLogging;
import xqa.commons.MessageSender;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.UnsupportedEncodingException;
import java.util.UUID;

class MockShard extends Thread implements Runnable, MessageListener {
    private static final Logger logger = LoggerFactory.getLogger(MockShard.class);
    public String digestOfMostRecentMessage;
    private boolean stop = false;
    private Destination insertUuidDestination;

    public MockShard() {
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
    }

    private void registerListeners() {
        try {
            Context context = new InitialContext();
            ConnectionFactory factory = (ConnectionFactory) context.lookup("url.amqp");
            Connection connection = factory.createConnection("admin", "admin");
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Destination cmdStopDestination = session.createTopic("xqa.cmd.stop");
            MessageConsumer cmdStopConsumer = session.createConsumer(cmdStopDestination);
            cmdStopConsumer.setMessageListener(this);

            Destination sizeDestination = session.createTopic("xqa.shard.size");
            MessageConsumer sizeConsumer = session.createConsumer(sizeDestination);
            sizeConsumer.setMessageListener(this);

            String uuid = UUID.randomUUID().toString().split("-")[0];
            synchronized (this) {
                insertUuidDestination = session.createQueue("xqa.shard.insert.".concat(uuid));
                MessageConsumer insertUuidConsumer = session.createConsumer(insertUuidDestination);
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
            JmsBytesMessage jmsBytesMessage = (JmsBytesMessage) message;
            switch (jmsBytesMessage.getJMSDestination().toString()) {
                case "xqa.cmd.stop": {
                    logger.debug(MessageLogging.log(MessageLogging.Direction.RECEIVE, message, false));
                    stop = true;
                    break;
                }

                case "xqa.shard.size": {
                    logger.debug(MessageLogging.log(MessageLogging.Direction.RECEIVE, message, false));
                    sendSizeReply(jmsBytesMessage);
                    break;
                }

                default:
                    logger.debug(MessageLogging.log(MessageLogging.Direction.RECEIVE, message, true));
                    synchronized (this) {
                        digestOfMostRecentMessage = DigestUtils.sha256Hex(MessageLogging.getTextFromMessage(message));
                    }
                    break;
            }

        } catch (Exception exception) {
            logger.error(exception.getMessage());
            exception.printStackTrace();
            System.exit(1);
        }
    }

    private void sendSizeReply(JmsBytesMessage jmsBytesMessage) throws JMSException, UnsupportedEncodingException, NamingException {
        MessageSender messageSender = new MessageSender();
        Destination insertDestination;
        synchronized (this) {
            insertDestination = this.insertUuidDestination;
            messageSender.sendReplyMessage(jmsBytesMessage.getJMSReplyTo(), jmsBytesMessage.getJMSCorrelationID(), insertDestination, "0", DeliveryMode.NON_PERSISTENT);
        }
    }
}
