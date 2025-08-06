import org.example.akka.extra.SystemConnector;
import org.example.akka.message.Response;

/**
 * Dummy implementation of SystemConnector.Listener with empty method bodies.
 * Used to isolate ScannerActor logic from external callbacks.
 */
// Dummy implementations
class DummyListener implements SystemConnector.Listener {
    public void onMessage(Response response) {}
    public void onConnect() {}
    public void onDisconnect() {}
    public void onOccupationChanged(boolean occupied){};
}