package xqa;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import xqa.commons.MessageLogging;
import xqa.commons.Properties;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

@SuppressWarnings("restriction")
public class IngestBalancer extends Thread implements MessageListener {
    private static final Logger logger = LoggerFactory.getLogger(IngestBalancer.class);
    private boolean stop = false;
    private ThreadPoolExecutor ingestPoolExecutor;

    public IngestBalancer() {
        logger.info(this.getClass().getSimpleName());
        setName("IngestBalancer");
    }

    public static void main(String... args) throws InterruptedException {
        Signal.handle(new Signal("INT"), signal -> {
            System.exit(1);
        });

        IngestBalancer ingestBalancer = new IngestBalancer();
        ingestBalancer.start();
        ingestBalancer.join();
    }

    private void initialiseIngestPool() throws IOException {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("InserterThread-%d")
                .setDaemon(true)
                .build();

        ingestPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.valueOf(Properties.getValue("xqa.ingest.balancer.pool.size")), threadFactory);
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

    private void registerListeners() throws NamingException, JMSException {
        Context context = new InitialContext();
        ConnectionFactory factory = (ConnectionFactory) context.lookup("url.amqp");
        Connection connection = factory.createConnection("admin", "admin");
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination cmdStop = session.createTopic("xqa.cmd.stop");
        MessageConsumer cmdStopConsumer = session.createConsumer(cmdStop);
        cmdStopConsumer.setMessageListener(this);

        Destination ingest = session.createQueue("xqa.ingest");
        MessageConsumer ingestConsumer = session.createConsumer(ingest);
        ingestConsumer.setMessageListener(this);
    }

    public void onMessage(Message message) {
        try {
            switch (message.getJMSDestination().toString()) {
                case "xqa.cmd.stop": {
                    logger.info(MessageLogging.log(MessageLogging.Direction.RECEIVE, message, false));
                    stop = true;
                    break;
                }

                case "xqa.ingest": {
                    logger.debug(MessageLogging.log(MessageLogging.Direction.RECEIVE, message, true));
                    ingestPoolExecutor.execute(new InserterThread(message));
                    break;
                }
            }
        } catch (Exception exception) {
            logger.error(exception.getMessage());
            exception.printStackTrace();
            System.exit(1);
        }
    }
}
