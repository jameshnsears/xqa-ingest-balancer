package xqa;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xqa.commons.MessageLogging;
import xqa.commons.MessageSender;
import xqa.commons.Properties;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.Vector;

class InserterThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(IngestBalancer.class);
    private final Message ingestMessage;
    public final Vector<Message> shardSizeResponses;

    public InserterThread(Message ingestMessage) {
        setName("InserterThread");
        synchronized (this) {
            this.ingestMessage = ingestMessage;
            shardSizeResponses = new Vector<>();
        }
    }

    public void run() {
        String sha256 = "";
        try {
            synchronized(this) {
                sha256 = DigestUtils.sha256Hex(MessageLogging.getTextFromMessage(ingestMessage));
                String s = MessageFormat.format("\"correlation_id\":\"{0}\", \"sha256\":\"{1}\", \"state\":\"S\"", ingestMessage.getJMSCorrelationID(), sha256);
                logger.info("{" + s + "}");
            }

            size();
            Message smallestShard = smallestShard();
            if (smallestShard != null) {
                insert(smallestShard);
            }
        } catch (Exception exception) {
            logger.error(exception.getMessage());
            exception.printStackTrace();
            System.exit(1);
        } finally {
            synchronized(this) {
                try {
                    String s = MessageFormat.format("\"correlation_id\":\"{0}\", \"sha256\":\"{1}\", \"state\":\"E\"", ingestMessage.getJMSCorrelationID(), sha256);
                    logger.info("{" + s + "}");
                } catch (Exception exception) {
                    logger.error(exception.getMessage());
                    exception.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }

    private void size() throws NamingException, JMSException, IOException {
        Context context = new InitialContext();

        ConnectionFactory factory = (ConnectionFactory) context.lookup("url.amqp");

        Connection connection = factory.createConnection("admin", "admin");
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination sizeDestination = session.createTopic("xqa.shard.size");
        Destination sizeReplyToDestination = session.createTemporaryQueue();

        sendSizeRequest(session, sizeDestination, sizeReplyToDestination);

        getSizeResponses(session, sizeReplyToDestination);

        session.close();
        connection.close();
    }

    private void getSizeResponses(Session session, Destination sizeReplyToDestination) throws JMSException, IOException {
        MessageConsumer messageConsumer = session.createConsumer(sizeReplyToDestination);

        int sizeResponseWaitTimeout;

        synchronized (this) {
            sizeResponseWaitTimeout = Integer.valueOf(Properties.getValue("xqa.ingest.balancer.shard.response.timeout"));
            logger.debug(MessageFormat.format("{0}: receive.START", ingestMessage.getJMSCorrelationID()));
        }

        Message sizeResponse = messageConsumer.receive(sizeResponseWaitTimeout);
        while (sizeResponse != null) {
            synchronized (this) {
                logger.debug(MessageLogging.log(MessageLogging.Direction.RECEIVE, sizeResponse, false));
                shardSizeResponses.add(sizeResponse);
            }
            sizeResponse = messageConsumer.receive(1000);
        }

        synchronized (this) {
            logger.debug(MessageFormat.format("{0}: receive.END; shardSizeResponses={1}", ingestMessage.getJMSCorrelationID(), shardSizeResponses.size()));

            if (shardSizeResponses.size() == 0) {
                logger.warn(MessageFormat.format("{0}: shardSizeResponses={1}", ingestMessage.getJMSCorrelationID(), shardSizeResponses.size()));
            }
        }

        messageConsumer.close();
    }

    private void sendSizeRequest(Session session, Destination sizeDestination, Destination sizeReplyToDestination) throws JMSException, UnsupportedEncodingException {
        MessageProducer messageProducer = session.createProducer(sizeDestination);
        BytesMessage sizeRequest;
        synchronized (this) {
            sizeRequest = MessageSender.constructMessage(session, sizeDestination, ingestMessage.getJMSCorrelationID(), sizeReplyToDestination, null);
            logger.debug(MessageLogging.log(MessageLogging.Direction.SEND, sizeRequest, false));
        }
        messageProducer.send(sizeRequest, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        messageProducer.close();
    }

    public Message smallestShard() throws JMSException, UnsupportedEncodingException {
        Message smallestShard = null;

        if (shardSizeResponses.size() > 0) {
            smallestShard = shardSizeResponses.get(0);

            for (Message currentShard : shardSizeResponses) {
                String textFromSmallestShard = MessageLogging.getTextFromMessage(smallestShard);
                String textFromCurrentShard = MessageLogging.getTextFromMessage(currentShard);
                if (Integer.valueOf(textFromSmallestShard) > Integer.valueOf(textFromCurrentShard)) {
                    smallestShard = currentShard;
                }
            }
        }

        return smallestShard;
    }

    private void insert(Message smallestShard) throws JMSException, UnsupportedEncodingException, NamingException {
        String correlationID;
        String text;
        synchronized (this) {
            correlationID = this.ingestMessage.getJMSCorrelationID();
            text = MessageLogging.getTextFromMessage(this.ingestMessage);
        }

        MessageSender messageSender = new MessageSender();
        messageSender.sendMessage(MessageSender.DestinationType.Queue, smallestShard.getJMSReplyTo().toString(), correlationID, null, text, DeliveryMode.PERSISTENT, true);
    }
    
    public String toString() {
        String r = "";
        synchronized (this) {
            try {
                r = (MessageFormat.format("{0}: sha256={1}", ingestMessage.getJMSCorrelationID(),
                        DigestUtils.sha256Hex(MessageLogging.getTextFromMessage(ingestMessage))));
            } catch (UnsupportedEncodingException | JMSException exception) {
                logger.error(exception.getMessage());
                exception.printStackTrace();
                System.exit(1);
            }
        }
        return r;
    }
}
