import org.example.akka.extra.Request;
import org.example.akka.extra.SystemConnector;
import org.example.akka.message.Response;

/**
 * Dummy connector for SystemConnector interface.
 * Returns static values to simulate always-connected behavior.
 */
 class DummyConnector implements SystemConnector {
    public boolean connect() { return true; }
    public boolean disconnect() { return true; }
    public boolean connected() { return true; }
    public Response send(Request request) { return new Response("Dummy", false, request.getId()); }
    public boolean addListener(Listener listener) { return false; }
    public boolean removeListener(Listener listener) { return false; }
}