import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;


public class LocalDynamoDBServer {

    private static DynamoDBProxyServer server;

    @SuppressWarnings("EmptyCatchBlock")
    public static void main(String[] args) throws Exception {
        final Thread currentThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.stop();
                currentThread.join();
            } catch (Exception ex) {
            }
        }));
        server = ServerRunner.createServerFromCommandLineArgs(args);
        server.start();
    }
}
