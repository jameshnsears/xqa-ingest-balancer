package xqa;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import xqa.commons.qpid.jms.MessageBroker;
import xqa.commons.qpid.jms.MessageLogger;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;


public class IngestBalancer extends Thread implements MessageListener {
    private static final Logger logger = LoggerFactory.getLogger(IngestBalancer.class);
    public final String serviceId;

    public MessageBroker messageBroker;
    public String messageBrokerHost;
    public String destinationIngest;
    public String destinationEvent;
    public String destinationShardSize;
    public String destinationCmdStop = "xqa.cmd.stop";
    public int poolSize;
    private int messageBrokerPort;
    private String messageBrokerUsername;
    private String messageBrokerPassword;
    private int messageBrokerRetryAttempts;
    private boolean stop = false;
    private ThreadPoolExecutor ingestPoolExecutor;

    public IngestBalancer() {
        serviceId = this.getClass().getSimpleName().toLowerCase() + "/" + UUID.randomUUID().toString().split("-")[0];
        logger.info(serviceId);
        setName("IngestBalancer");
    }

    public static void main(String[] args) throws ParseException, InterruptedException, CommandLineException {
        Signal.handle(new Signal("INT"), signal -> System.exit(1));

        try {
            IngestBalancer ingestBalancer = new IngestBalancer();
            ingestBalancer.processCommandLine(args);
            ingestBalancer.start();
            ingestBalancer.join();
        } catch (Exception exception) {
            logger.error(exception.getMessage());
            throw exception;
        }
    }

    public void processCommandLine(String[] args) throws ParseException, CommandLineException {
        Options options = new Options();

        options.addOption("message_broker_host", true, "i.e. xqa-message-broker");
        options.addOption("message_broker_port", true, "i.e. 5672");
        options.addOption("message_broker_username", true, "i.e. admin");
        options.addOption("message_broker_password", true, "i.e. admin");
        options.addOption("message_broker_retry", true, "i.e. 3");

        options.addOption("destination_ingest", true, "i.e. xqa.ingest");
        options.addOption("destination_event", true, "i.e. xqa.event");
        options.addOption("destination_shard_size", true, "i.e. xqa.shard.size");

        options.addOption("pool_size", true, "i.e. 4");

        CommandLineParser commandLineParser = new DefaultParser();
        setConfigurationValues(options, commandLineParser.parse(options, args));
    }

    private void setConfigurationValues(Options options, CommandLine commandLine) throws CommandLineException {
        if (commandLine.hasOption("message_broker_host")) {
            messageBrokerHost = commandLine.getOptionValue("message_broker_host");
            logger.info("message_broker_host=" + messageBrokerHost);
        } else {
            showUsage(options);
        }

        messageBrokerPort = Integer.parseInt(commandLine.getOptionValue("message_broker_port", "5672"));
        messageBrokerUsername = commandLine.getOptionValue("message_broker_username", "admin");
        messageBrokerPassword = commandLine.getOptionValue("message_broker_password", "admin");
        messageBrokerRetryAttempts = Integer.parseInt(commandLine.getOptionValue("message_broker_retry", "3"));

        destinationIngest = commandLine.getOptionValue("destination_ingest", "xqa.ingest");
        destinationEvent = commandLine.getOptionValue("destination_event", "xqa.event");
        destinationShardSize = commandLine.getOptionValue("destination_shard_size", "xqa.shard.size");

        poolSize = Integer.parseInt(commandLine.getOptionValue("pool_size", "1"));
    }

    private void showUsage(final Options options) throws CommandLineException {
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("IngestBalancer", options);
        throw new IngestBalancer.CommandLineException();
    }

    private void initialiseIngestPool() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("InserterThread-%d").setDaemon(true)
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
        } catch (Exception exception) {
            logger.error(exception.getMessage());
            exception.printStackTrace();
            System.exit(1);
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

        Destination cmdStop = messageBroker.getSession().createTopic(destinationCmdStop);
        MessageConsumer cmdStopConsumer = messageBroker.getSession().createConsumer(cmdStop);
        cmdStopConsumer.setMessageListener(this);

        Destination ingest = messageBroker.getSession().createQueue(destinationIngest);
        MessageConsumer ingestConsumer = messageBroker.getSession().createConsumer(ingest);
        ingestConsumer.setMessageListener(this);

        logger.info("listeners registered");
    }

    public void onMessage(Message message) {
        try {
            if (message.getJMSDestination().toString().equals(destinationCmdStop)) {
                logger.info(MessageLogger.log(MessageLogger.Direction.RECEIVE, message, false));
                stop = true;
            }

            if (message.getJMSDestination().toString().equals(destinationIngest)) {
                logger.info(MessageLogger.log(MessageLogger.Direction.RECEIVE, message, true));
                ingestPoolExecutor.execute(new InserterThread(
                        serviceId,
                        messageBroker,
                        message,
                        poolSize,
                        destinationEvent,
                        destinationShardSize));
            }
        } catch (Exception exception) {
            logger.error(exception.getMessage());
            exception.printStackTrace();
            System.exit(1);
        }
    }

    @SuppressWarnings("serial")
    public class CommandLineException extends Exception {
        public CommandLineException() {
        }
    }
}
