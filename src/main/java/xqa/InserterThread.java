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

    private final String serviceId;
    private final MessageBroker messageBroker;
    private final Message ingestMessage;
    private final int poolSize;

    private final String destinationEvent;
    private String destinationSize;

    public InserterThread(String serviceId,
                          MessageBroker messageBroker,
                          Message ingestMessage,
                          int poolSize,
                          String destinationEvent,
                          String destinationSize) {
        setName("InserterThread");

        synchronized (this) {
            this.serviceId = serviceId;
            this.messageBroker = messageBroker;
            this.ingestMessage = ingestMessage;
            this.poolSize = poolSize;

            this.destinationEvent = destinationEvent;
            this.destinationSize = destinationSize;
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
            exception.printStackTrace();
            System.exit(1);
        }
    }

    private void sendEventToMessageBroker(final String state) throws Exception {
        Message message = MessageMaker.createMessage(
                messageBroker.getSession(),
                messageBroker.getSession().createQueue(destinationEvent),
                UUID.randomUUID().toString(),
                new Gson().toJson(
                        new IngestBalancerEvent(
                                serviceId,
                                ingestMessage.getJMSCorrelationID(),
                                poolSize,
                                DigestUtils.sha256Hex(MessageMaker.getBody(ingestMessage)),
                                state)));

        messageBroker.sendMessage(message);
    }

    private List<Message> askShardsForSize() throws Exception {
        MessageBroker mb = new MessageBroker("0.0.0.0", 5672, "admin", "admin", 3);

        TemporaryQueue sizeReplyToDestination;

        synchronized (this) {
            sizeReplyToDestination = mb.createTemporaryQueue();

            Message message = MessageMaker.createMessage(
                    mb.getSession(),
                    mb.getSession().createTopic(destinationSize),
                    sizeReplyToDestination,
                    UUID.randomUUID().toString(),
                    "");
            mb.sendMessage(message);
        }

        List<Message> shardSizeResponses = getSizeResponses(mb, sizeReplyToDestination);
        mb.close();
        return shardSizeResponses;
    }

    private synchronized List<Message> getSizeResponses(
            MessageBroker mb,
            TemporaryQueue sizeReplyToDestination) throws Exception {

        logger.debug(MessageFormat.format("{0}: START", ingestMessage.getJMSCorrelationID()));

        List<Message> shardSizeResponses = mb.receiveMessagesTemporaryQueue(sizeReplyToDestination, 6000);

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

        synchronized (this) {
            if (shardSizeResponses.size() > 0) {
                smallestShard = shardSizeResponses.get(0);

                for (Message currentShard : shardSizeResponses) {
                    if (Integer.valueOf(
                            MessageMaker.getBody(smallestShard)) > Integer.valueOf(MessageMaker.getBody(currentShard))) {
                        smallestShard = currentShard;
                    }
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
