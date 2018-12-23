package xqa;

import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.List;
import java.util.UUID;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TemporaryQueue;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import xqa.commons.qpid.jms.MessageBroker;
import xqa.commons.qpid.jms.MessageMaker;

class InserterThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(InserterThread.class);
    static int threadInstances = 0;
    private final IngestBalancer ingestBalancer;
    private final Message ingestMessage;
    private MessageBroker inserterThreadMessageBroker;

    InserterThread(IngestBalancer ingestBalancer, Message ingestMessage) {
        setName("InserterThread");
        synchronized (this) {
            this.ingestBalancer = ingestBalancer;
            this.ingestMessage = ingestMessage;
        }
    }

    public void run() {
        threadInstances++;

        try {
            synchronized (this) {
                logger.debug(MessageFormat.format("++++++++++++ {0}: {1}", threadInstances,
                        ingestMessage.getJMSCorrelationID()));

                inserterThreadMessageBroker = new MessageBroker(ingestBalancer.messageBrokerHost,
                        ingestBalancer.messageBrokerPort, ingestBalancer.messageBrokerUsername,
                        ingestBalancer.messageBrokerPassword, ingestBalancer.messageBrokerRetryAttempts);
            }

            sendEventToMessageBroker("START");

            Message smallestShard = findSmallestShard(askShardsForSize());
            if (smallestShard != null) {
                insert(smallestShard);
            }

            sendEventToMessageBroker("END");

            synchronized (this) {
                inserterThreadMessageBroker.close();
            }
        } catch (Exception exception) {
            logger.error(exception.getMessage());
            System.exit(1);
        }

        logger.debug("++++++++++++");
    }

    private synchronized void sendEventToMessageBroker(final String state) throws Exception {
        Message message = MessageMaker.createMessage(inserterThreadMessageBroker.getSession(),
                inserterThreadMessageBroker.getSession().createQueue(ingestBalancer.destinationEvent),
                UUID.randomUUID().toString(),
                new Gson().toJson(new IngestBalancerEvent(ingestBalancer.serviceId, ingestMessage.getJMSCorrelationID(),
                        ingestBalancer.poolSize, DigestUtils.sha256Hex(MessageMaker.getBody(ingestMessage)), state)));

        inserterThreadMessageBroker.sendMessage(message);
    }

    private synchronized List<Message> askShardsForSize() throws Exception {
        TemporaryQueue sizeReplyToDestination = inserterThreadMessageBroker.createTemporaryQueue();

        Message message = MessageMaker.createMessage(inserterThreadMessageBroker.getSession(),
                inserterThreadMessageBroker.getSession().createTopic(ingestBalancer.destinationShardSize),
                sizeReplyToDestination, UUID.randomUUID().toString(), "");
        inserterThreadMessageBroker.sendMessage(message);

        return getSizeResponses(inserterThreadMessageBroker, sizeReplyToDestination);
    }

    private synchronized List<Message> getSizeResponses(MessageBroker shardSizeMessageBroker,
            TemporaryQueue sizeReplyToDestination) throws Exception {

        logger.debug(MessageFormat.format("{0}: START", ingestMessage.getJMSCorrelationID()));

        List<Message> shardSizeResponses = shardSizeMessageBroker.receiveMessagesTemporaryQueue(sizeReplyToDestination,
                ingestBalancer.insertThreadWait, ingestBalancer.insertThreadSecondaryWait);

        if (shardSizeResponses.size() == 0) {
            logger.warn(MessageFormat.format("{0}: END: shardSizeResponses=None; subject={1}",
                    ingestMessage.getJMSCorrelationID(), ingestMessage.getJMSType()));

            placeMessageBackOnOriginatingDestination();
        } else
            logger.debug(MessageFormat.format("{0}: END: shardSizeResponses={1}", ingestMessage.getJMSCorrelationID(),
                    shardSizeResponses.size()));

        return shardSizeResponses;
    }

    private synchronized void placeMessageBackOnOriginatingDestination()
            throws JMSException, UnsupportedEncodingException, MessageBroker.MessageBrokerException {
        Message message =
                MessageMaker.createMessage(
                        inserterThreadMessageBroker.getSession(),
                        inserterThreadMessageBroker.getSession().createQueue("xqa.ingest"),
                        ingestMessage.getJMSCorrelationID(),
                        ingestMessage.getJMSType(),
                        MessageMaker.getBody(ingestMessage));

        logger.warn(message.getJMSCorrelationID());
        logger.warn(message.getJMSType());
        logger.warn(DigestUtils.sha256Hex(MessageMaker.getBody(message)));

        inserterThreadMessageBroker.sendMessage(message);
    }

    public Message findSmallestShard(List<Message> shardSizeResponses) throws Exception {
        Message smallestShard = null;

        if (shardSizeResponses.size() > 0) {
            smallestShard = shardSizeResponses.get(0);

            for (Message currentShard : shardSizeResponses) {
                if (Integer.valueOf(MessageMaker.getBody(smallestShard)) > Integer
                        .valueOf(MessageMaker.getBody(currentShard))) {
                    smallestShard = currentShard;
                }
            }
        }

        return smallestShard;
    }

    private synchronized void insert(final Message smallestShard) throws Exception {
        Message message = MessageMaker.createMessage(inserterThreadMessageBroker.getSession(),
                inserterThreadMessageBroker.getSession().createQueue(smallestShard.getJMSReplyTo().toString()),
                ingestMessage.getJMSCorrelationID(), MessageMaker.getSubject(ingestMessage),
                MessageMaker.getBody(ingestMessage));

        inserterThreadMessageBroker.sendMessage(message);
    }
}
