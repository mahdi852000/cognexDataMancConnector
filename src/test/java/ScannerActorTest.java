import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import lombok.extern.slf4j.Slf4j;
import net.enilink.komma.core.*;
import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.actor.dmcc.ScannerActor;
import org.example.akka.config.RangeObserverConfig;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.extra.*;
import org.example.akka.message.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Slf4j
public class ScannerActorTest {

    static final ActorTestKit testKit = ActorTestKit.create();
    private ActorRef<ScannerCommand> scannerActor;
    private final TestProbe<String> scanReceiverProbe = testKit.createTestProbe();

    private ActorRef<ScannerCommand> spawnScannerActor(SystemConnector.Listener listener) {
        TestProbe<CognexCommand> fakeCognexActor = testKit.createTestProbe(CognexCommand.class);
        ScannerActorConfig config = new ScannerActorConfig(
                1,
                new DummyDMCC(),
                listener,
                new DummyResource(),
                "localhost",
                5000,
                fakeCognexActor.getRef(),
                true,
                scanReceiverProbe.getRef(),
                false
        );
        return testKit.spawn(ScannerActor.create(config), "Scanner-" + UUID.randomUUID());
    }

    @AfterAll
    static void tearDown() {
        testKit.shutdownTestKit();
    }

    @Test
    public void testQueryIsConnectedShouldReturnFalseInitially() {
        scannerActor = spawnScannerActor(new DummyListener());
        TestProbe<ScannerCommand.ConnectedStatus> probe = testKit.createTestProbe();
        scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.getRef()));
        assertFalse(probe.receiveMessage().status());
    }

    @Test
    public void testSetAndQueryOccupation() {
        scannerActor = spawnScannerActor(new DummyListener());
        TestProbe<ScannerCommand.OccupationStatus> probe = testKit.createTestProbe();

        scannerActor.tell(new ScannerCommand.SetOccupation(true));
        probe.awaitAssert(Duration.ofSeconds(3), () -> {
            scannerActor.tell(new ScannerCommand.QueryOccupation(probe.getRef()));
            assertTrue(probe.receiveMessage().occupied());
            return null;
        });

        scannerActor.tell(new ScannerCommand.SetOccupation(false));
        probe.awaitAssert(Duration.ofSeconds(3), () -> {
            scannerActor.tell(new ScannerCommand.QueryOccupation(probe.getRef()));
            assertFalse(probe.receiveMessage().occupied());
            return null;
        });
    }

    @Test
    public void testOnConnectShouldUpdateConnectionStatus() {
        URI mockUri = mock(URI.class);
        when(mockUri.toString()).thenReturn("urn:dummy");

        IReference mockRef = mock(IReference.class);
        when(mockRef.getURI()).thenReturn(mockUri);

        IResource mockResource = mock(IResource.class);
        when(mockResource.getSingle(any())).thenReturn(mockRef);
        when(mockResource.getReference()).thenReturn(mockRef);
        when(mockResource.getURI()).thenReturn(mockUri);

        TestProbe<ScannerCommand.ConnectedStatus> probe = testKit.createTestProbe();
        TestProbe<CognexCommand> fakeCognexActor = testKit.createTestProbe(CognexCommand.class);
        TestProbe<String> scanReceiverProbe = testKit.createTestProbe();

        ScannerActorConfig config = new ScannerActorConfig(
                1,
                new DummyDMCC(),
                new DummyListener(),
                mockResource,
                "localhost",
                5000,
                fakeCognexActor.getRef(),
                true,
                scanReceiverProbe.getRef(),
                false
        );

        scannerActor = testKit.spawn(ScannerActor.create(config), "Scanner-" + UUID.randomUUID());
        scannerActor.tell(new ScannerCommand.Connect());

        // انتظار داریم پیامی از نوع CognexCommand.Connect ارسال شود
        fakeCognexActor.expectMessageClass(CognexCommand.Connect.class);
        log.info("hey rooozegar");

        probe.awaitAssert(Duration.ofSeconds(3), () -> {
            scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.getRef()));
            assertTrue(probe.receiveMessage().status());
            return null;
        });
    }

    @Test
    public void testOnDisconnectShouldUpdateConnectionStatus() {
        scannerActor = spawnScannerActor(new DummyListener());
        scannerActor.tell(new ScannerCommand.OnConnect());

        TestProbe<ScannerCommand.ConnectedStatus> probe = testKit.createTestProbe();

        // Wait until actor has processed OnConnect and status becomes true
        probe.awaitAssert(Duration.ofSeconds(3), ()-> {
            scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.getRef()));
            assertTrue(probe.receiveMessage().status());
            return null;
        });


        scannerActor.tell(new ScannerCommand.OnDisconnect());
        // Wait until actor has processed OnDisconnect and status becomes false
        probe.awaitAssert(Duration.ofSeconds(3), () -> {
            scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.getRef()));
            assertFalse(probe.receiveMessage().status());
            return null;
        });
    }

    @Test
    public void testRangeObserverToScannerActorIntegration() {
        long[] fakeHeights = {120L, 130L, 140L};
        DummyDMCC dynamicDMCC = new DummyDMCC(fakeHeights);
        TestProbe<ScannerCommand> scannerProbe = testKit.createTestProbe();
        TestProbe<String> scanReceiverProbe = testKit.createTestProbe();

        RangeObserverConfig rangeConfig = new RangeObserverConfig(
                dynamicDMCC, 1, 100L, 160L, 180L,
                scannerProbe.getRef(), "fakeUri", "localhost", 5000, scanReceiverProbe.getRef()
        );

        ActorRef<RangeObserverCommand> observer = testKit.spawn(
                RangeObserverActor.create(rangeConfig),
                "RangeObserver-" + UUID.randomUUID()
        );

        observer.tell(new RangeObserverCommand.StartObserving());

        ScannerCommand.SetOccupation occ = scannerProbe.expectMessageClass(
                ScannerCommand.SetOccupation.class, Duration.ofSeconds(5));
        assertTrue(occ.occupied());

        ScannerCommand.TriggerScan triggerScan = scannerProbe.expectMessageClass(
                ScannerCommand.TriggerScan.class, Duration.ofSeconds(10));
        assertNotNull(triggerScan);
    }
    /**
     * Verifies that the ScannerActor properly cleans up its resources upon receiving a Disconnect command.
     * <p>
     * This includes:
     * - Stopping observation by sending StopObserving to the range observer actor.
     * - Removing the listener from the DataMan system.
     * - Calling disconnect on the DataMan system.
     * <p>
     * The test uses manual initialization of internal fields (e.g., `connected`, `rangeObserverActor`)
     * to simulate the actor being in a connected state prior to disconnecting.
     */

    @Test
    void testOnDisconnectShouldCleanupResources() {
        //creating test probe
        TestProbe<RangeObserverCommand> rangeObserverProbe = testKit.createTestProbe();
        TestProbe<CognexCommand> cognexProbe = testKit.createTestProbe();

        // mocking dependencies
        DataManSystem dmccMock = mock(DataManSystem.class);
        SystemConnector.Listener listenerMock = mock(SystemConnector.Listener.class);
        IResource delegateMock = mock(IResource.class);

        // simulating  an already-connected state
        when(dmccMock.connected()).thenReturn(true);

        // Creating a sample configuration for ScannerActor
        ScannerActorConfig config = new ScannerActorConfig(
                41,
                dmccMock,
                listenerMock,
                delegateMock,
                "localhost",
                1234,
                cognexProbe.getRef(),
                false,
                scanReceiverProbe.getRef(),
                false
    );
        // Spawning the actor with overridden internal state
        Behavior<ScannerCommand> behavior = Behaviors.setup(ctx ->
                new ScannerActor(ctx, config, cognexProbe.getRef()) {
                    {
                        // Manually initializing internal state for test
                        this.connected = true;
                        this.rangeObserverActor = rangeObserverProbe.getRef();
                        this.isRangeObserving = true;
                    }
                }
        );
        ActorRef<ScannerCommand> actor = testKit.spawn(behavior);

        // Sending the Disconnect command
        actor.tell(new ScannerCommand.Disconnect());

        // Verifying that StopObserving was sent to the range observer
        rangeObserverProbe.expectMessageClass(RangeObserverCommand.StopObserving.class);

        // Verifying listener removal and disconnect calls on the DataMan system
        verify(dmccMock).removeListener(listenerMock);
        verify(dmccMock).disconnect();
        // Since `connected` is a private field, we can't verify it directly.
        // However, calling `disconnect` and `removeListener` implies it was handled correctly.

    }

    // Dummy implementations
    static class DummyListener implements SystemConnector.Listener {
        public void onMessage(Response response) {}
        public void onConnect() {}
        public void onDisconnect() {}
    }

    static class DummyDMCC extends DataManSystem {
        private boolean connected = false;
        private final long[] simulatedHeights;
        private int index = 0;

        public DummyDMCC() {
            super(new DummyConnector());
            this.simulatedHeights = new long[]{140};
        }

        public DummyDMCC(long[] simulatedHeights) {
            super(new DummyConnector());
            this.simulatedHeights = simulatedHeights;
        }

        public Response send(Request request) {
            long value = simulatedHeights[index % simulatedHeights.length];
            index++;
            return new Response(Long.toString(value), false, request.getId());
        }

        public Response sendCommand(String command, Integer id, boolean log) {
            long value = simulatedHeights[index % simulatedHeights.length];
            index++;
            return new Response(Long.toString(value), false, id);
        }

        public boolean connected() { return connected; }
        public boolean connect() { return connected = true; }
    }

    static class DummyConnector implements SystemConnector {
        public boolean connect() { return true; }
        public boolean disconnect() { return true; }
        public boolean connected() { return true; }
        public Response send(Request request) { return new Response("Dummy", false, request.getId()); }
        public boolean addListener(Listener listener) { return false; }
        public boolean removeListener(Listener listener) { return false; }
    }

    static class DummyResource implements IResource {
        private final IReference ref;
        private final URI uri;

        public DummyResource() {
            this(mock(URI.class));
        }

        public DummyResource(URI uri) {
            this.uri = uri;
            ref = mock(IReference.class);
            when(ref.getURI()).thenReturn(uri);
        }

        public Object getSingle(IReference var1) { return ref; }
        public <T> T as(Class<T> aClass) { return null; }
        public IEntityManager getEntityManager() { return null; }
        public void refresh() {}
        public URI getURI() { return uri; }
        public IReference getReference() { return ref; }
    }
}
