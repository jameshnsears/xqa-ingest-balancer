package xqa;

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
import xqa.commons.qpid.jms.MessageBroker.MessageBrokerException;
import xqa.commons.qpid.jms.MessageMaker;

class InserterThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(InserterThread.class);
    private static int threadInstances;
    private final IngestBalancer ingestBalancer;
    private final Message ingestMessage;
    private MessageBroker inserterThreadMessageBroker;

    InserterThread(final IngestBalancer ingestBalancer, final Message ingestMessage) {
        setName("InserterThread");
        synchronized (this) {
            this.ingestBalancer = ingestBalancer;
            this.ingestMessage = ingestMessage;
        }
    }

    @Override
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

            final Message smallestShard = findSmallestShard(askShardsForSize());
            if (smallestShard != null) {
                insert(smallestShard);
            }

            sendEventToMessageBroker("END");

            synchronized (this) {
                inserterThreadMessageBroker.close();
            }
        } catch (Exception exception) {
            logger.error(exception.getMessage());
        }

        logger.debug("++++++++++++");
    }

    private void sendEventToMessageBroker(final String state) throws JMSException, MessageBrokerException {
        synchronized (this) {
            final Message message = MessageMaker.createMessage(inserterThreadMessageBroker.getSession(),
                    inserterThreadMessageBroker.getSession().createQueue(ingestBalancer.destinationEvent),
                    UUID.randomUUID().toString(),
                    new Gson().toJson(new IngestBalancerEvent(ingestBalancer.serviceId, ingestMessage.getJMSCorrelationID(),
                            ingestBalancer.poolSize, DigestUtils.sha256Hex(MessageMaker.getBody(ingestMessage)), state)));

            inserterThreadMessageBroker.sendMessage(message);
        }
    }

    private synchronized List<Message> askShardsForSize() throws Exception {
        final TemporaryQueue sizeReplyToDestination = inserterThreadMessageBroker.createTemporaryQueue();

        final Message message = MessageMaker.createMessage(inserterThreadMessageBroker.getSession(),
                inserterThreadMessageBroker.getSession().createTopic(ingestBalancer.destinationShardSize),
                sizeReplyToDestination, UUID.randomUUID().toString(), "");
        inserterThreadMessageBroker.sendMessage(message);

        return getSizeResponses(inserterThreadMessageBroker, sizeReplyToDestination);
    }

    private synchronized List<Message> getSizeResponses(final MessageBroker shardSizeMessageBroker,
            final TemporaryQueue sizeReplyTo) throws JMSException, MessageBrokerException {

        logger.debug(MessageFormat.format("{0}: START", ingestMessage.getJMSCorrelationID()));

        final List<Message> sizeResponses = shardSizeMessageBroker.receiveMessagesTemporaryQueue(sizeReplyTo,
                ingestBalancer.insertThreadWait, ingestBalancer.insertThreadSecondaryWait);

        if (sizeResponses.isEmpty()) {
            logger.warn(MessageFormat.format("{0}: END: shardSizeResponses=None; subject={1}",
                    ingestMessage.getJMSCorrelationID(), ingestMessage.getJMSType()));

            placeMessageBackOnOriginatingDestination();
        } else {
            logger.debug(MessageFormat.format("{0}: END: shardSizeResponses={1}", ingestMessage.getJMSCorrelationID(),
                    sizeResponses.size()));
        }

        return sizeResponses;
    }

    private void placeMessageBackOnOriginatingDestination() throws JMSException, MessageBroker.MessageBrokerException {
        logger.warn("placeMessageBackOnOriginatingDestination");

        synchronized (this) {
            final Message message = MessageMaker.createMessage(inserterThreadMessageBroker.getSession(),
                    inserterThreadMessageBroker.getSession().createQueue("xqa.ingest"),
                    ingestMessage.getJMSCorrelationID(), ingestMessage.getJMSType(),
                    MessageMaker.getBody(ingestMessage));

            logger.warn(message.getJMSCorrelationID());
            logger.warn(message.getJMSType());
            logger.warn(DigestUtils.sha256Hex(MessageMaker.getBody(message)));

            inserterThreadMessageBroker.sendMessage(message);
        }
    }

    public Message findSmallestShard(final List<Message> sizeResponses) throws JMSException {
        Message smallestShard = null;

        if (!sizeResponses.isEmpty()) {
            smallestShard = sizeResponses.get(0);

            for (final Message currentShard : sizeResponses) {
                if (Integer.valueOf(MessageMaker.getBody(smallestShard)) > Integer
                        .valueOf(MessageMaker.getBody(currentShard))) {
                    smallestShard = currentShard;
                }
            }
        }

        return smallestShard;
    }

    private void insert(final Message smallestShard) throws JMSException, MessageBrokerException {
        synchronized (this) {
            final Message message = MessageMaker.createMessage(inserterThreadMessageBroker.getSession(),
                    inserterThreadMessageBroker.getSession().createQueue(smallestShard.getJMSReplyTo().toString()),
                    ingestMessage.getJMSCorrelationID(), MessageMaker.getSubject(ingestMessage),
                    MessageMaker.getBody(ingestMessage));

            inserterThreadMessageBroker.sendMessage(message);
        }
    }
}
