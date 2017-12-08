package xqa.commons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.UnsupportedEncodingException;
import java.util.Date;

public class MessageSender {
    private static final Logger logger = LoggerFactory.getLogger(MessageSender.class);

    public static BytesMessage constructMessage(Session session,
                                                Destination ingest,
                                                String correlationID,
                                                Destination replyTo,
                                                String body) throws JMSException {
        BytesMessage message = session.createBytesMessage();
        message.setJMSDestination(ingest);
        message.setJMSCorrelationID(correlationID);
        message.setJMSTimestamp(new Date().getTime());
        message.setJMSReplyTo(replyTo);
        if (body != null) {
            message.writeBytes(body.getBytes());
        }

        return message;
    }

    public void sendMessage(DestinationType destinationType, String destinationName, String correlationID, Destination replyTo, String body, int deliveryMode, boolean useDigest) throws NamingException, JMSException, UnsupportedEncodingException {
        Context context = new InitialContext();

        ConnectionFactory factory = (ConnectionFactory) context.lookup("url.amqp");

        Connection connection = factory.createConnection("admin", "admin");
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination destination;
        if (destinationType == DestinationType.Queue) {
            destination = session.createQueue(destinationName);
        } else {
            destination = session.createTopic(destinationName);
        }
        MessageProducer messageProducer = session.createProducer(destination);

        BytesMessage message = constructMessage(session, destination, correlationID, replyTo, body);
        logger.debug(MessageLogging.log(MessageLogging.Direction.SEND, message, useDigest));

        messageProducer.send(message,
                deliveryMode, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

        messageProducer.close();
        session.close();
        connection.close();
    }

    public void sendReplyMessage(Destination destination, String correlationID, Destination replyTo, String body, int deliveryMode) throws NamingException, JMSException, UnsupportedEncodingException {
        Context context = new InitialContext();

        ConnectionFactory factory = (ConnectionFactory) context.lookup("url.amqp");

        Connection connection = factory.createConnection("admin", "admin");
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer messageProducer = session.createProducer(null);

        BytesMessage replyMessage = constructMessage(session, destination, correlationID, replyTo, body);
        logger.debug(MessageLogging.log(MessageLogging.Direction.SEND, replyMessage, false));

        messageProducer.send(destination, replyMessage, deliveryMode, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

        messageProducer.close();
        session.close();
        connection.close();
    }

    public enum DestinationType {
        Queue, Topic
    }
}
