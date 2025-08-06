import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
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


import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * This test class validates the behavior of ScannerActor and its integration with RangeObserverActor
 * using Akka Typed's ActorTestKit. It includes:
 * - Unit tests for verifying state transitions (connection, occupation).
 * - Integration test with RangeObserverActor to simulate real-time height updates.
 * - Use of dummy/mock implementations for external dependencies (DMCC, resource, listener).
 * - Ensures scanner actor's proper interaction with CognexCommand and scan receivers.
 */
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

    /**
     * Test that verifies the initial connection status of ScannerActor is false (disconnected).
     */
    @Test
    public void testQueryIsConnectedShouldReturnFalseInitially() {
        scannerActor = spawnScannerActor(new DummyListener());
        TestProbe<ScannerCommand.ConnectedStatus> probe = testKit.createTestProbe();
        scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.getRef()));
        ScannerCommand.ConnectedStatus result = probe.receiveMessage();
        assertFalse(result.status());
        log.info("Actor Status is : {}", result.status());//However this is always false
    }

    /**
     * Tests the occupation status of ScannerActor.
     * - Sets occupation to true and verifies.
     * - Sets it back to false and verifies again.
     * Useful for simulating sensor occupation behavior.
     */
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
    /**
     * Tests ScannerActor's behavior upon receiving a Connect command.
     * - Uses a mock IResource to simulate reading the URI.
     * - Expects a CognexCommand.Connect message to be sent.
     * - Then queries internal connection status and expects it to be true.
     */
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

        // Spawn a ScannerActor instance and send a Connect command.
        // We expect the actor to send a CognexCommand.Connect message to the fake Cognex actor,
        // indicating it is attempting to establish a connection.
        // Then we query the connection status and assert that the actor reports it as connected.
        fakeCognexActor.expectMessageClass(CognexCommand.Connect.class);
        log.info("hey rooozegar");

        probe.awaitAssert(Duration.ofSeconds(3), () -> {
            scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.getRef()));
            assertTrue(probe.receiveMessage().status());
            return null;
        });
    }
    /**
     * Tests the disconnection flow:
     * - Simulates OnConnect, verifies status is true.
     * - Then simulates OnDisconnect, expects status to become false.
     * Ensures ScannerActor correctly tracks and reports connection state.
     */
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
    /**
     * Full integration test between RangeObserverActor and ScannerActor (via a TestProbe).
     * - Simulates a dynamic DMCC that returns varying height values.
     * - Observer monitors range, then sends appropriate SetOccupation and TriggerScan commands to ScannerActor.
     * - Verifies both commands are received correctly.
     */
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
}
