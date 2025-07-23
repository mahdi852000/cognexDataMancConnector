import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import net.enilink.komma.core.*;
import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.actor.dmcc.ScannerActor;
import org.example.akka.config.RangeObserverConfig;
import org.example.akka.extra.IResource;
import org.example.akka.extra.Request;
import org.example.akka.extra.SystemConnector;
import org.example.akka.message.CognexCommands;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.Response;
import org.example.akka.message.ScannerCommand;
import org.example.akka.extra.DataManSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import net.enilink.komma.core.IReference;

import static org.mockito.Mockito.*;

import net.enilink.komma.core.URI;


import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ScannerActorTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    private  ActorRef<ScannerCommand> scannerActor;
            //= testKit.spawn(
            //ScannerActor.create(1, null, null, null, "localhost", 5000, null), "scanner-1"
            //);
            TestProbe<String> scanReceiverProbe = testKit.createTestProbe();


    private ActorRef<ScannerCommand> spawnScannerActor(SystemConnector.Listener listener) {
        TestProbe<CognexCommands.CognexCommand> fakeCognexActor = testKit.createTestProbe();

        return testKit.spawn(
                ScannerActor.create(
                        1,
                        new DummyDMCC(),
                        listener,
                        new DummyResource(),
                        "localhost",
                        5000,
                        fakeCognexActor.getRef(),
                        true,
                        scanReceiverProbe.getRef()

                ),
                "Scanner-" + UUID.randomUUID()
        );
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
        ScannerCommand.ConnectedStatus status = probe.receiveMessage();
        assertFalse(status.status());
    }

    @Test
    // Test for SetOccupation and Occupation Check
    //Making sure that when a SetOccupation(true) or SetOccupation(false) message is received,
    // the occupation value in ScannerActor is correctly updated and returned in response to a QueryOccupation.

    public void testSetAndQueryOccupation() {
        scannerActor = spawnScannerActor(new DummyListener());
        TestProbe<ScannerCommand.OccupationStatus> probe = testKit.createTestProbe();

        scannerActor.tell(new ScannerCommand.SetOccupation(true));
        scannerActor.tell(new ScannerCommand.QueryOccupation(probe.getRef()));
        assertTrue(probe.receiveMessage().occupied());

        scannerActor.tell(new ScannerCommand.SetOccupation(false));
        scannerActor.tell(new ScannerCommand.QueryOccupation(probe.getRef()));
        assertFalse(probe.receiveMessage().occupied());
    }

    //Test the Connect message and check whether isConnected is updated correctly.
    //However, since ScannerActor uses classes like DataManSystem and SystemConnector.Listener
    //for actual connections, we need to provide a simple mock or fake implementation for them.

    @Test
    public void testOnConnectShouldUpdateConnectionStatus() {
        // ساخت URI mock
        URI mockUri = mock(URI.class);
        when(mockUri.toString()).thenReturn("urn:dummy");


        IReference mockRef = mock(IReference.class);
        when(mockRef.getURI()).thenReturn(mockUri);


        IResource customResource = new IResource() {
            @Override
            public Object getSingle(IReference var1) {
                return mockRef;
            }

            @Override
            public <T> T as(Class<T> aClass) {
                return null;
            }

            @Override
            public IEntityManager getEntityManager() {
                return null;
            }

            @Override
            public void refresh() {

            }

            @Override
            public URI getURI() {
                return null;
            }

            @Override
            public IReference getReference() {
                return mockRef;
            }
        };

        // ساخت IReference mock

        // ساخت IResource mock و پیاده‌سازی getSingle
        IResource mockResource = mock(IResource.class);
        when(mockResource.getSingle(any())).thenReturn(mockRef);
        when(mockResource.getReference()).thenReturn(mockRef);


        when(mockResource.getURI()).thenReturn(mockUri);

        // حالا ScannerActor رو بساز
        TestProbe<ScannerCommand.ConnectedStatus> probe = testKit.createTestProbe();
        TestProbe<CognexCommands.CognexCommand> fakeCognexActor =
                testKit.createTestProbe(CognexCommands.CognexCommand.class);
        TestProbe<String> scanReceiverProbe = testKit.createTestProbe();


        scannerActor = testKit.spawn(
                ScannerActor.create (
                        1,
                        new DummyDMCC(),
                        new DummyListener(),
                        mockResource, // ✅ حالا این همونی‌یه که ScannerActor می‌خواست
                        "localhost",
                        5000,
                        fakeCognexActor.getRef(),
                        true,
                        scanReceiverProbe.getRef()

                ),
                "Scanner-" + UUID.randomUUID()
        );

        // تست اصلی
        scannerActor.tell(new ScannerCommand.OnConnect());

        fakeCognexActor.awaitAssert(Duration.ofSeconds(3), () -> {
            scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.getRef()));
            ScannerCommand.ConnectedStatus status = probe.receiveMessage();
            assertTrue(status.status()); // اگر true نباشد، خودش چند بار retry می‌کند
            return null;
        });

    }

    // only for TEST purpose

    static class DummyListener implements SystemConnector.Listener {
        @Override public void onMessage(Response response) {}
        @Override public void onConnect() {}
        @Override public void onDisconnect() {}
    }
    static class DummyDMCC extends DataManSystem {

        private boolean connected = false;
        private final long[]  simulatedHeights;
        private int index=0;

        public DummyDMCC() {
            super(new DummyConnector());
            this.simulatedHeights = new long[]{140}; // یه مقدار پیش‌فرض
        }

        public DummyDMCC(long[] simulatedHeights) {
            super(new DummyConnector());
            this.simulatedHeights=simulatedHeights;
        }

        public Response send(Request request) {
            long value = simulatedHeights[index % simulatedHeights.length];
            index++;
            return new Response(Long.toString(value), false, request.getId());
        }

        @Override
        public Response sendCommand(String command, Integer id, boolean log) throws IOException {
            long value = simulatedHeights[index % simulatedHeights.length];
            index++;
            return new  Response(Long.toString(value),false,id);
        }

        @Override
        public boolean connected() {
            System.out.println("DummyDMCC.connected() called");
            return connected;
        }

        public boolean connect(){
            connected = true;
            return true;
        }
    }

    static class DummyConnector implements SystemConnector {
        @Override
        public boolean connect() {
            return true;
        }

        @Override
        public boolean disconnect() {
            return true;
        }

        @Override
        public boolean connected() {
            return true;
        }

        @Override
        public Response send(Request request) {
            return new Response ("Dummy response", false, request.getId());
        }

        @Override
        public boolean addListener(Listener listener) {
            return false;
        }

        @Override
        public boolean removeListener(Listener listener) {
            return false;
        }
    }
    static class DummyResource implements IResource {

        private final IReference ref;
        private final URI uri;

        public  DummyResource() {
            this(mock(URI.class));
        }

        public DummyResource(URI uri) {
            this.uri = uri;
            ref=mock(IReference.class);
            when(ref.getURI()).thenReturn(uri);

        }

        public IReference getBehaviorDelegate() {
            return ref;
        }
        @Override
        public Object getSingle(IReference var1) {

            return ref;
        }

        @Override
        public <T> T as(Class<T> aClass) {
            return null;
        }

        @Override
        public IEntityManager getEntityManager() {
            return null;
        }

        @Override
        public void refresh() {

        }

        @Override
        public URI getURI() {
            return uri;
        }

        @Override
        public IReference getReference() {
            return ref;
        }
    }

    @Test
    public void testRangeObserverToScannerActorIntegration() {
        long[] fakeHeights = { 120L, 130L, 140L };
        DummyDMCC dynamicDMCC = new DummyDMCC(fakeHeights);
        TestProbe<ScannerCommand> scannerProbe = testKit.createTestProbe(ScannerCommand.class);
        TestProbe<String> scanReceiverProbe = testKit.createTestProbe();

        long rangeMin = 100L;
        long rangeMax = 160L;
        long rangeOff = 180L;
        int cmId = 1;
        String uri = "fakeUri";
        String host = "localhost";
        int port = 5000;

        RangeObserverConfig rangeConfig = new RangeObserverConfig(
                dynamicDMCC, cmId, rangeMin, rangeMax, rangeOff,
                scannerProbe.getRef(), uri, host, port, scanReceiverProbe.getRef()
        );

        ActorRef<RangeObserverCommand> observer = testKit.spawn(
                RangeObserverActor.create(rangeConfig),
                "RangeObserver-" + UUID.randomUUID()
        );

        observer.tell(new RangeObserverCommand.StartObserving());

        ScannerCommand.SetOccupation occ = scannerProbe.expectMessageClass(
                ScannerCommand.SetOccupation.class,
                Duration.ofSeconds(5)
        );
        assertTrue(occ.occupied(), "Occupation should be ON based on simulated average height");

        ScannerCommand.TriggerScan triggerScan = scannerProbe.expectMessageClass(
                ScannerCommand.TriggerScan.class, Duration.ofSeconds(10)
        );
        assertNotNull(triggerScan);
    }


    @Test
    public void testOnDisconnectShouldUpdateConnectionStatus() {
        // ابتدا بازیگر اسکنر را با وضعیت متصل ساخته و متصل کنیم
        scannerActor = spawnScannerActor(new DummyListener());

        // ابتدا اتصال را شبیه‌سازی می‌کنیم (می‌توانیم پیام OnConnect بفرستیم)
        scannerActor.tell(new ScannerCommand.OnConnect());

        // چک می‌کنیم که وضعیت اتصال true شده
        TestProbe<ScannerCommand.ConnectedStatus> probe = testKit.createTestProbe();
        scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.getRef()));
        ScannerCommand.ConnectedStatus status = probe.receiveMessage();
        assertTrue(status.status(), "Scanner should be connected after OnConnect");

        // حالا پیام قطع اتصال را می‌فرستیم
        scannerActor.tell(new ScannerCommand.OnDisconnect());

        // دوباره وضعیت اتصال را می‌پرسیم و انتظار داریم false باشد
        scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.getRef()));
        ScannerCommand.ConnectedStatus disconnectedStatus = probe.receiveMessage();
        assertFalse(disconnectedStatus.status(), "Scanner should be disconnected after OnDisconnect");
    }


}



