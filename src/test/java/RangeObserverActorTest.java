import akka.actor.testkit.typed.javadsl.*;
import akka.actor.typed.ActorRef;

import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.config.RangeObserverConfig;
import org.example.akka.extra.DataManSystem;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.Response;
import org.example.akka.message.ScannerCommand;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RangeObserverActorTest {

    private static ActorTestKit testKit;
    private static final int cmId = 1;
    private static final long rangeMin = 10L;
    private static final long rangeMax = 100L;
    private static final long rangeOff = 120L;
    private static final String uri = "fakeUri";
    private static final String host = "localhost";
    private static final int port = 5000;


    @BeforeAll
    static void setup() {
        testKit = ActorTestKit.create();
    }

    @AfterAll
    static void tearDown() {
        testKit.shutdownTestKit();
    }
    /**
     * Tests the complete behavior of the RangeObserverActor when started, ticked, and stopped.
     * <p>
     * It verifies:
     * <ul>
     *   <li>That the actor sends a scan trigger message when the measured distance is within range</li>
     *   <li>That occupation is correctly reported to the scanner actor</li>
     *   <li>That scan triggering stops after receiving a StopObserving command</li>
     *   <li>That the DataManSystem is correctly invoked to retrieve distance measurements</li>
     * </ul>
     */

    @Test
    void testRangeObserverActorBehavior() throws IOException {
        // Create a mock for DataManSystem, which provides distance measurements
        DataManSystem dmccMock = Mockito.mock(DataManSystem.class);

        // Create a mock for the response returned by DataManSystem
        Response responseMock = Mockito.mock(Response.class);

        // Configure the mock system to return the mock response when a command is sent
        when(dmccMock.sendCommand(anyString(), anyInt(), anyBoolean())).thenReturn(responseMock);

        // Simulate a valid response with a numeric distance value "50"
        when(responseMock.result()).thenReturn("50");

        // Create a test probe to observe messages sent to the scanner actor
        TestProbe<ScannerCommand> scannerProbe = testKit.createTestProbe();

        // Create a test probe to receive scan results
        TestProbe<String> scanReceiverProbe = testKit.createTestProbe();

        // Build the configuration and spawn the RangeObserverActor with mock dependencies
        RangeObserverConfig config = new RangeObserverConfig(
                dmccMock, cmId, rangeMin, rangeMax, rangeOff,
                scannerProbe.getRef(), uri, host, port, scanReceiverProbe.getRef()
        );
        ActorRef<RangeObserverCommand> rangeObserverActor = testKit.spawn(
                RangeObserverActor.create(config)
        );

        // Start observing (enables ticking and distance checks)
        rangeObserverActor.tell(new RangeObserverCommand.StartObserving());

        // Manually trigger a tick (simulates a distance check)
        rangeObserverActor.tell(new RangeObserverCommand.Tick());

        // Expect the actor to mark itself as occupied
        ScannerCommand.SetOccupation setOcc = scannerProbe.expectMessageClass(ScannerCommand.SetOccupation.class);
        assertTrue(setOcc.occupied());

        // Expect the actor to trigger a scan
        ScannerCommand triggerScan = scannerProbe.expectMessageClass(ScannerCommand.TriggerScan.class);
        assertNotNull(triggerScan);

        // Stop observing (disables further ticking behavior)
        rangeObserverActor.tell(new RangeObserverCommand.StopObserving());

        // Send another Tick, which should now be ignored
        rangeObserverActor.tell(new RangeObserverCommand.Tick());

        // Ensure no further messages are sent after StopObserving
        scannerProbe.expectNoMessage();

        // Verify that sendCommand was called at least once to fetch distance
        verify(dmccMock, atLeastOnce()).sendCommand(anyString(), anyInt(), anyBoolean());
    }

    /**
     * Verifies that the RangeObserverActor correctly forwards scanned codes
     * to the configured scan result receiver.
     * <p>
     * This test simulates receiving a scan code while the observer is active
     * and checks that the code is passed to the external receiver as expected.
     */

    @Test
    void testRangeObserverReceivesScanCode() {

        // Create a test probe to simulate the ScannerActor (receives internal commands)
        TestProbe<ScannerCommand> scannerProbe = testKit.createTestProbe();

        // Create a test probe to receive the scan results from the observer
        TestProbe<String> scanResultReceiver = testKit.createTestProbe();

        // Mock the DataManSystem dependency (not used in this test but required for config)
        DataManSystem dmcc = mock(DataManSystem.class);

        // Prepare the actor configuration
        RangeObserverConfig config = new RangeObserverConfig(
                dmcc, 1, 100L, 200L, 300L,
                scannerProbe.getRef(), "uri", "host", 1234, scanResultReceiver.getRef()

        );
        // Spawn the RangeObserverActor with the given config
        ActorRef<RangeObserverCommand> observer = testKit.spawn(
                RangeObserverActor.create(config)
        );
       // observer.tell(new RangeObserverCommand.StartObserving());

        // Simulate reception of a scan code by sending ScanCode message
        String scannedCode = "abc123";
        observer.tell(new RangeObserverCommand.ScanCode(scannedCode));

        // Verify that the scan code is forwarded to the receiver
        String receivedCode = scanResultReceiver.receiveMessage();
        assertEquals(scannedCode, receivedCode);
    }

}
