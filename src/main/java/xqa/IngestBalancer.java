package xqa;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import xqa.commons.qpid.jms.MessageBroker;
import xqa.commons.qpid.jms.MessageLogger;


public class IngestBalancer extends Thread implements MessageListener {
    private static final Logger logger = LoggerFactory.getLogger(IngestBalancer.class);
    public final String serviceId;

    private MessageBroker messageBroker;
    public String messageBrokerHost;
    public String destinationIngest;
    public String destinationEvent;
    public String destinationShardSize;
    public final String destinationCmdStop = "xqa.cmd.stop";
    public int poolSize;
    public int messageBrokerPort;
    public String messageBrokerUsername;
    public String messageBrokerPassword;
    public int messageBrokerRetryAttempts;
    public int insertThreadWait;
    public int insertThreadSecondaryWait;
    private boolean stop;
    private ThreadPoolExecutor ingestPoolExecutor;

    public IngestBalancer() {
        serviceId = this.getClass().getSimpleName().toLowerCase() + "/" + UUID.randomUUID().toString().split("-")[0];
        logger.info(serviceId);
        setName("IngestBalancer");
    }

    public static void main(final String... args) throws ParseException, InterruptedException {
        final IngestBalancer ingestBalancer = new IngestBalancer();
        ingestBalancer.processCommandLine(args);
        ingestBalancer.start();
        ingestBalancer.join();
    }

    public void processCommandLine(final String... args) throws ParseException {
        final Options options = new Options();

        options.addOption("message_broker_host", true, "i.e. xqa-message-broker");
        options.addOption("message_broker_port", true, "i.e. 5672");
        options.addOption("message_broker_username", true, "i.e. admin");
        options.addOption("message_broker_password", true, "i.e. admin");
        options.addOption("message_broker_retry", true, "i.e. 3");

        options.addOption("destination_ingest", true, "i.e. xqa.ingest");
        options.addOption("destination_event", true, "i.e. xqa.event");
        options.addOption("destination_shard_size", true, "i.e. xqa.shard.size");

        options.addOption("pool_size", true, "i.e. 4");

        options.addOption("insert_thread_wait", true, "i.e. 10000");            // 10 seconds
        options.addOption("insert_thread_secondary_wait", true, "i.e. 1000");   // 1 second

        final CommandLineParser commandLineParser = new DefaultParser();
        setConfigurationValues(options, commandLineParser.parse(options, args));
    }

    private void setConfigurationValues(final Options options, final CommandLine commandLine) {
        if (commandLine.hasOption("message_broker_host")) {
            messageBrokerHost = commandLine.getOptionValue("message_broker_host");
            logger.info("message_broker_host=" + messageBrokerHost);
        }

        messageBrokerPort = Integer.parseInt(commandLine.getOptionValue("message_broker_port", "5672"));
        messageBrokerUsername = commandLine.getOptionValue("message_broker_username", "admin");
        messageBrokerPassword = commandLine.getOptionValue("message_broker_password", "admin");
        messageBrokerRetryAttempts = Integer.parseInt(commandLine.getOptionValue("message_broker_retry", "10"));

        destinationIngest = commandLine.getOptionValue("destination_ingest", "xqa.ingest");
        destinationEvent = commandLine.getOptionValue("destination_event", "xqa.event");
        destinationShardSize = commandLine.getOptionValue("destination_shard_size", "xqa.shard.size");

        insertThreadSecondaryWait = Integer.parseInt(commandLine.getOptionValue("insert_thread_secondary_wait", "5000"));
        logger.info("insert_thread_secondary_wait=" + insertThreadSecondaryWait);

        insertThreadWait = Integer.parseInt(commandLine.getOptionValue("insert_thread_wait", "60000"));
        logger.info("insert_thread_wait=" + insertThreadWait);

        poolSize = Integer.parseInt(commandLine.getOptionValue("pool_size", "4"));
        logger.info("pool_size=" + poolSize);
    }

    private void initialiseIngestPool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("InserterThread-%d").setDaemon(true)
                .build();

        ingestPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(poolSize, threadFactory);
    }

    public void run() {
        try {
            initialiseIngestPool();
            registerListeners();

            while (!stop) {
                Thread.sleep(500);
            }

            messageBroker.close();
        } catch (Exception exception) {
            logger.error(exception.getMessage());
        } finally {
            ingestPoolExecutor.shutdown();
        }
    }

    private void registerListeners() throws Exception {
        messageBroker = new MessageBroker(
                messageBrokerHost,
                messageBrokerPort,
                messageBrokerUsername,
                messageBrokerPassword,
                messageBrokerRetryAttempts);

        final Destination cmdStop = messageBroker.getSession().createTopic(destinationCmdStop);
        final MessageConsumer cmdStopConsumer = messageBroker.getSession().createConsumer(cmdStop);
        cmdStopConsumer.setMessageListener(this);

        final Destination ingest = messageBroker.getSession().createQueue(destinationIngest);
        final MessageConsumer ingestConsumer = messageBroker.getSession().createConsumer(ingest);
        ingestConsumer.setMessageListener(this);

        logger.info("listeners registered");
    }

    public void onMessage(final Message message) {
        try {
            if (message.getJMSDestination().toString().equals(destinationCmdStop)) {
                logger.info(MessageLogger.log(MessageLogger.Direction.RECEIVE, message, false));
                stop = true;
            }

            if (message.getJMSDestination().toString().equals(destinationIngest)) {
                logger.info(MessageLogger.log(MessageLogger.Direction.RECEIVE, message, true));
                ingestPoolExecutor.execute(new InserterThread(this, message));
            }
        } catch (Exception exception) {
            logger.error(exception.getMessage());
        }
    }
}
