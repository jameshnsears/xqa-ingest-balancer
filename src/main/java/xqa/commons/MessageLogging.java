package xqa.commons;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsObjectMessage;
import org.apache.qpid.jms.message.JmsTextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;

public class MessageLogging {
    private static final Logger logger = LoggerFactory.getLogger(MessageLogging.class);
    
    public static String log(Direction direction, Message message, boolean useDigest) throws JMSException, UnsupportedEncodingException {
        String arrowDirection = getArrow(direction);
        String jmsTimestamp = Long.toString(message.getJMSTimestamp());
        String jmsCorrelationID = message.getJMSCorrelationID();
        Destination jmsDestination = message.getJMSDestination();
        Destination jmsReplyTo = message.getJMSReplyTo();
        long jmsExpiration = message.getJMSExpiration();
        String text = getTextFromMessage(message);

        return formmatedLog(arrowDirection, jmsTimestamp, jmsCorrelationID, jmsDestination, jmsReplyTo, jmsExpiration, text, useDigest);
    }

    public static String getTextFromMessage(Message message) throws JMSException, UnsupportedEncodingException {
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
            return new String(serializable.toString().getBytes(),"UTF-8");
        } else {
            logger.debug("JmsTextMessage"); 
            JmsTextMessage jmsTextMessage = (JmsTextMessage) message;
            return new String(jmsTextMessage.getText().getBytes(),"UTF-8");
        }
    }

    private static String formmatedLog(String arrowDirection, String jmsTimestamp, String jmsCorrelationID, Destination jmsDestination, Destination jmsReplyTo, long jmsExpiration, String text, boolean useDigest) {
        String commonLogString = MessageFormat.format(
                "{0} jmsTimestamp={1}; jmsCorrelationID={2}; jmsDestination={3}; jmsReplyTo={4}; jmsExpiration={5}",
                arrowDirection,
                jmsTimestamp,
                jmsCorrelationID,
                jmsDestination,
                jmsReplyTo,
                jmsExpiration);

        String testLogString = "";
        if (useDigest) {
            testLogString = MessageFormat.format("; sha256(text)={0}", DigestUtils.sha256Hex(text));
        } else {
            if (text != null && !text.equals("")) {
                testLogString = MessageFormat.format("; text={0}", text);
            }
        }

        return commonLogString.concat(testLogString);
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
