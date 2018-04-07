package xqa;

import com.google.gson.Gson;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.qpid.jms.message.JmsBytesMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xqa.commons.IngestBalancerConnectionFactory;
import xqa.commons.MessageLogging;
import xqa.commons.MessageSender;

import javax.jms.*;
import java.text.MessageFormat;
import java.util.UUID;
import java.util.Vector;

public class InserterThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(IngestBalancer.class);
    public final Vector<Message> shardSizeResponses;
    private final String serviceId;
    private final String messageBrokerHost;
    private final Message ingestMessage;
    private MessageSender messageSender;
    private int poolSize;

    public InserterThread(String serviceId, String messageBrokerHost, int poolSize, Message ingestMessage) {
        setName("InserterThread");
        synchronized (this) {
            this.serviceId = serviceId;
            this.poolSize = poolSize;
            this.messageBrokerHost = messageBrokerHost;
            this.ingestMessage = ingestMessage;
            shardSizeResponses = new Vector<>();
        }
    }

    public void run() {
        try {
            synchronized (this) {
                messageSender = new MessageSender(messageBrokerHost);

                sendEventToMessageBroker(
                        new IngestBalancerEvent(serviceId, ingestMessage.getJMSCorrelationID(), this.poolSize,
                                DigestUtils.sha256Hex(MessageLogging.getTextFromMessage(ingestMessage)), "START"));
            }

            size();
            Message smallestShard = smallestShard();
            if (smallestShard != null) {
                insert(smallestShard, MessageLogging.getTextFromMessage(ingestMessage));
            }

            synchronized (this) {
                sendEventToMessageBroker(new IngestBalancerEvent(serviceId, ingestMessage.getJMSCorrelationID(),
                        this.poolSize, DigestUtils.sha256Hex(MessageLogging.getTextFromMessage(ingestMessage)), "END"));

                messageSender.close();
            }
        } catch (Exception exception) {
            logger.error(exception.getMessage());
            exception.printStackTrace();
            System.exit(1);
        }
    }

    private void sendEventToMessageBroker(final IngestBalancerEvent ingestBalancerEvent) throws Exception {
        BytesMessage messageSent = messageSender.sendMessage(MessageSender.DestinationType.Queue,
                "xqa.db.amqp.insert_event", UUID.randomUUID().toString(), null, null,
                new Gson().toJson(ingestBalancerEvent), DeliveryMode.PERSISTENT);
        logger.debug(MessageLogging.log(MessageLogging.Direction.SEND, messageSent, true));

    }

    private void size() throws Exception {
        ConnectionFactory factory = IngestBalancerConnectionFactory.messageBroker(messageBrokerHost);

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

    private void getSizeResponses(Session session, Destination sizeReplyToDestination) throws Exception {
        MessageConsumer messageConsumer = session.createConsumer(sizeReplyToDestination);

        synchronized (this) {
            logger.debug(MessageFormat.format("{0}: getSizeResponses.START", ingestMessage.getJMSCorrelationID()));
        }

        Message sizeResponse = messageConsumer.receive(60000);
        while (sizeResponse != null) {
            synchronized (this) {
                logger.debug(MessageLogging.log(MessageLogging.Direction.RECEIVE, sizeResponse, false));
                shardSizeResponses.add(sizeResponse);
            }
            sizeResponse = messageConsumer.receive(5000);
        }

        synchronized (this) {
            logger.debug(MessageFormat.format("{0}: getSizeResponses.END; shardSizeResponses={1}",
                    ingestMessage.getJMSCorrelationID(), shardSizeResponses.size()));

            if (shardSizeResponses.size() == 0) {
                logger.warn(MessageFormat.format("{0}: shardSizeResponses={1}; subject={2}",
                        ingestMessage.getJMSCorrelationID(),
                        shardSizeResponses.size(),
                        ingestMessage.getJMSType()));
            }
        }

        messageConsumer.close();
    }

    private void sendSizeRequest(Session session, Destination sizeDestination, Destination sizeReplyToDestination)
            throws Exception {
        MessageProducer messageProducer = session.createProducer(sizeDestination);
        BytesMessage sizeRequest;
        synchronized (this) {
            sizeRequest = MessageSender.constructMessage(session, sizeDestination, ingestMessage.getJMSCorrelationID(),
                    sizeReplyToDestination, null, null);
            logger.debug(MessageLogging.log(MessageLogging.Direction.SEND, sizeRequest, false));
        }
        messageProducer.send(sizeRequest, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY,
                Message.DEFAULT_TIME_TO_LIVE);
        messageProducer.close();
    }

    public Message smallestShard() throws Exception {
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

    private void insert(Message smallestShard, String text) throws Exception {
        String correlationID;
        String subject;
        synchronized (this) {
            correlationID = this.ingestMessage.getJMSCorrelationID();
            JmsBytesMessage jmsBytesMessage = (JmsBytesMessage) this.ingestMessage;
            subject = jmsBytesMessage.getFacade().getType();
        }

        MessageSender messageSender = new MessageSender(messageBrokerHost);
        BytesMessage messageSent = messageSender.sendMessage(MessageSender.DestinationType.Queue,
                smallestShard.getJMSReplyTo().toString(), correlationID, null, subject, text, DeliveryMode.PERSISTENT);
        logger.info(MessageLogging.log(MessageLogging.Direction.SEND, messageSent, true));

        messageSender.close();
    }
}
