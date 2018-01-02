package xqa.commons;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsObjectMessage;
import org.apache.qpid.jms.message.JmsTextMessage;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsBytesMessageFacade;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsObjectMessageFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Destination;
import javax.jms.Message;
import java.io.Serializable;
import java.text.MessageFormat;

public class MessageLogging {
    private static final Logger logger = LoggerFactory.getLogger(MessageLogging.class);

    public static String log(Direction direction,
                             Message message,
                             boolean useDigest) throws Exception {
        String arrowDirection = getArrow(direction);
        String jmsTimestamp = Long.toString(message.getJMSTimestamp());
        String jmsCorrelationID = message.getJMSCorrelationID();
        Destination jmsDestination = message.getJMSDestination();
        Destination jmsReplyTo = message.getJMSReplyTo();
        long jmsExpiration = message.getJMSExpiration();
        String text = getTextFromMessage(message);
        String subject = getSubject(message);

        return formmatedLog(arrowDirection, jmsTimestamp, jmsCorrelationID, jmsDestination, jmsReplyTo, subject,
                jmsExpiration, text, useDigest);
    }

    private static String getSubject(Message message) {
        if (message instanceof JmsBytesMessage) {
            logger.debug("JmsBytesMessage");
            JmsBytesMessage jmsBytesMessage = (JmsBytesMessage) message;
            AmqpJmsBytesMessageFacade facade = (AmqpJmsBytesMessageFacade) jmsBytesMessage.getFacade();
            return facade.getType();
        } else if (message instanceof JmsObjectMessage) {
            logger.debug("JmsObjectMessage");
            JmsObjectMessage jmsObjectMessage = (JmsObjectMessage) message;
            AmqpJmsObjectMessageFacade facade = (AmqpJmsObjectMessageFacade) jmsObjectMessage.getFacade();
            return facade.getType();
        } else {
            logger.debug("JmsTextMessage");
            JmsTextMessage jmsTextMessage = (JmsTextMessage) message;
            JmsMessageFacade facade = jmsTextMessage.getFacade();
            return facade.getType();
        }
    }

    public static String getTextFromMessage(Message message) throws Exception {
        if (message instanceof JmsBytesMessage) {
            logger.debug("JmsBytesMessage");
            JmsBytesMessage jmsBytesMessage = (JmsBytesMessage) message;
            jmsBytesMessage.reset();
            byte[] byteData;
            byteData = new byte[(int) jmsBytesMessage.getBodyLength()];
            jmsBytesMessage.readBytes(byteData);
            return new String(byteData, "UTF-8");
        } else if (message instanceof JmsObjectMessage) {
            logger.debug("JmsObjectMessage");
            JmsObjectMessage jmsObjectMessage = (JmsObjectMessage) message;
            Serializable serializable = jmsObjectMessage.getObject();
            return new String(serializable.toString().getBytes(), "UTF-8");
        } else {
            logger.debug("JmsTextMessage");
            JmsTextMessage jmsTextMessage = (JmsTextMessage) message;
            return new String(jmsTextMessage.getText().getBytes(), "UTF-8");
        }
    }

    private static String formmatedLog(String arrowDirection,
                                       String jmsTimestamp,
                                       String jmsCorrelationID,
                                       Destination jmsDestination,
                                       Destination jmsReplyTo,
                                       String subject,
                                       long jmsExpiration,
                                       String text,
                                       boolean useDigest) {
        String commonLogString = MessageFormat.format(
                "{0} jmsTimestamp={1}; jmsDestination={2}; jmsCorrelationID={3}; jmsReplyTo={4}; subject={5}; jmsExpiration={6}",
                arrowDirection, jmsTimestamp, jmsDestination, jmsCorrelationID, jmsReplyTo, subject, jmsExpiration);

        String textLogString = "";
        if (useDigest) {
            textLogString = MessageFormat.format("; digest(text)={0}", DigestUtils.sha256Hex(text));
        } else {
            if (text != null && !text.equals("")) {
                textLogString = MessageFormat.format("; text={0}", text);
            }
        }

        return commonLogString.concat(textLogString);
    }

    private static String getArrow(Direction direction) {
        if (direction == Direction.SEND) {
            return "<";
        }
        return ">";
    }

    public enum Direction {
        SEND, RECEIVE
    }
}
