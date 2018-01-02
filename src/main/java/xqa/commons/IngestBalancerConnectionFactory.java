package xqa.commons;


import javax.jms.ConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Hashtable;

public class IngestBalancerConnectionFactory {
    static public ConnectionFactory messageBroker(final String messageBrokerHost) throws Exception {
        Hashtable<String, String> env = new Hashtable<>();
        env.put("connectionfactory.url.amqp", "amqp://" + messageBrokerHost + ":5672/");
        env.put("java.naming.factory.initial", "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        Context context = new InitialContext(env);

        return (ConnectionFactory) context.lookup("url.amqp");
    }
}
