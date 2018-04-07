package xqa;

import com.google.gson.Gson;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xqa.commons.qpid.jms.MessageBroker;
import xqa.commons.qpid.jms.MessageMaker;

import javax.jms.Message;
import javax.jms.TemporaryQueue;
import java.text.MessageFormat;
import java.util.List;
import java.util.UUID;

class InserterThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(InserterThread.class);

    private final IngestBalancer ingestBalancer;
    private final MessageBroker messageBroker;
    private final Message ingestMessage;

    public InserterThread(IngestBalancer ingestBalancer,
                          MessageBroker messageBroker,
                          Message ingestMessage) {
        setName("InserterThread");

        synchronized (this) {
            this.ingestBalancer = ingestBalancer;
            this.messageBroker = messageBroker;
            this.ingestMessage = ingestMessage;
        }
    }

    public void run() {
        try {
            sendEventToMessageBroker("START");

            Message smallestShard = findSmallestShard(askShardsForSize());
            if (smallestShard != null) {
                insert(smallestShard);
            }

            sendEventToMessageBroker("END");
        } catch (Exception exception) {
            logger.error(exception.getMessage());
            System.exit(1);
        }
    }

    private synchronized void sendEventToMessageBroker(final String state) throws Exception {
        Message message = MessageMaker.createMessage(
                messageBroker.getSession(),
                messageBroker.getSession().createQueue(ingestBalancer.destinationEvent),
                UUID.randomUUID().toString(),
                new Gson().toJson(
                        new IngestBalancerEvent(
                                ingestBalancer.serviceId,
                                ingestMessage.getJMSCorrelationID(),
                                ingestBalancer.poolSize,
                                DigestUtils.sha256Hex(MessageMaker.getBody(ingestMessage)),
                                state)));

        messageBroker.sendMessage(message);
    }

    private List<Message> askShardsForSize() throws Exception {
        MessageBroker shardSizeMessageBroker = null;
        try {
            shardSizeMessageBroker = new MessageBroker(
                    ingestBalancer.messageBrokerHost,
                    ingestBalancer.messageBrokerPort,
                    ingestBalancer.messageBrokerUsername,
                    ingestBalancer.messageBrokerPassword,
                    ingestBalancer.messageBrokerRetryAttempts);

            TemporaryQueue sizeReplyToDestination = shardSizeMessageBroker.createTemporaryQueue();

            synchronized (this) {
                Message message = MessageMaker.createMessage(
                        shardSizeMessageBroker.getSession(),
                        shardSizeMessageBroker.getSession().createTopic(ingestBalancer.destinationShardSize),
                        sizeReplyToDestination,
                        UUID.randomUUID().toString(),
                        "");
                shardSizeMessageBroker.sendMessage(message);
            }

            return getSizeResponses(shardSizeMessageBroker, sizeReplyToDestination);
        } finally {
            shardSizeMessageBroker.close();
        }
    }

    private synchronized List<Message> getSizeResponses(
            MessageBroker shardSizeMessageBroker,
            TemporaryQueue sizeReplyToDestination) throws Exception {

        logger.debug(MessageFormat.format("{0}: START", ingestMessage.getJMSCorrelationID()));

        List<Message> shardSizeResponses = shardSizeMessageBroker.receiveMessagesTemporaryQueue(
                sizeReplyToDestination,
                ingestBalancer.insertThreadWait);

        if (shardSizeResponses.size() == 0)
            logger.warn(MessageFormat.format("{0}: END: shardSizeResponses={1}; subject={2}",
                    ingestMessage.getJMSCorrelationID(),
                    shardSizeResponses.size(),
                    ingestMessage.getJMSType()));
        else
            logger.debug(MessageFormat.format("{0}: END: shardSizeResponses={1}",
                    ingestMessage.getJMSCorrelationID(),
                    shardSizeResponses.size()));

        return shardSizeResponses;
    }

    public Message findSmallestShard(List<Message> shardSizeResponses) throws Exception {
        Message smallestShard = null;

        if (shardSizeResponses.size() > 0) {
            smallestShard = shardSizeResponses.get(0);

            for (Message currentShard : shardSizeResponses) {
                if (Integer.valueOf(
                        MessageMaker.getBody(smallestShard)) > Integer.valueOf(MessageMaker.getBody(currentShard))) {
                    smallestShard = currentShard;
                }
            }
        }

        return smallestShard;
    }

    private void insert(final Message smallestShard) throws Exception {
        Message message = MessageMaker.createMessage(
                messageBroker.getSession(),
                smallestShard.getJMSReplyTo(),
                ingestMessage.getJMSCorrelationID(),
                MessageMaker.getBody(ingestMessage));

        messageBroker.sendMessage(message);
    }
}
