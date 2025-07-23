import akka.actor.testkit.typed.javadsl.*;
import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.Behaviors;
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

    @Test
    void testRangeObserverActorBehavior() throws IOException {
        // ساخت mock برای DataManSystem
        DataManSystem dmccMock = Mockito.mock(DataManSystem.class);

        // ساخت mock برای Response
        Response responseMock = Mockito.mock(Response.class);

        // وقتی sendCommand با هر آرگومانی صدا زده شود، مقدار responseMock را برگردان
        when(dmccMock.sendCommand(anyString(), anyInt(), anyBoolean())).thenReturn(responseMock);

        // مقداردهی به متد result در responseMock برای برگرداندن رشته عددی "50"
        when(responseMock.result()).thenReturn("50");

        // ساخت actor mock برای scannerActor که پیام‌ها به آن ارسال می‌شود
        TestProbe<ScannerCommand> scannerProbe = testKit.createTestProbe();

        TestProbe<String> scanReceiverProbe = testKit.createTestProbe();


        // ساخت actor تحت تست با مقادیر ورودی فرضی
        RangeObserverConfig config = new RangeObserverConfig(
                dmccMock, cmId, rangeMin, rangeMax, rangeOff,
                scannerProbe.getRef(), uri, host, port, scanReceiverProbe.getRef()
        );
        ActorRef<RangeObserverCommand> rangeObserverActor = testKit.spawn(
                RangeObserverActor.create(config)
        );

        // ارسال پیام StartObserving
        rangeObserverActor.tell(new RangeObserverCommand.StartObserving());

        // منتظر بمانیم تا پیام Tick خودکار از timer ارسال شود (یا می‌توانیم مستقیم پیام Tick بفرستیم)
        rangeObserverActor.tell(new RangeObserverCommand.Tick());

        // حالا انتظار داریم که scannerActor پیام SetOccupation(true) و TriggerScan دریافت کند
        ScannerCommand.SetOccupation setOcc = scannerProbe.expectMessageClass(ScannerCommand.SetOccupation.class);
        assertTrue(setOcc.occupied());

        ScannerCommand triggerScan = scannerProbe.expectMessageClass(ScannerCommand.TriggerScan.class);
        assertNotNull(triggerScan);

        // ارسال پیام StopObserving
        rangeObserverActor.tell(new RangeObserverCommand.StopObserving());

        // پس از Stop دیگر Tick تاثیری ندارد
        rangeObserverActor.tell(new RangeObserverCommand.Tick());

        // نباید پیام جدید به scannerProbe بیاید
        scannerProbe.expectNoMessage();

        // verify اینکه sendCommand حداقل یکبار صدا زده شده
        verify(dmccMock, atLeastOnce()).sendCommand(anyString(), anyInt(), anyBoolean());
    }


    @Test
    void testRangeObserverReceivesScanCode() {
       // TestKitJunitResource testKit = new TestKitJunitResource();

        // ScannerActor mock
        TestProbe<ScannerCommand> scannerProbe = testKit.createTestProbe();

        // Probe for receiving scan results
        TestProbe<String> scanResultReceiver = testKit.createTestProbe();

        DataManSystem dmcc = mock(DataManSystem.class);

        RangeObserverConfig config = new RangeObserverConfig(
                dmcc, 1, 100L, 200L, 300L,
                scannerProbe.getRef(), "uri", "host", 1234, scanResultReceiver.getRef()

        );


        // می‌سازیم actor اصلی با رفرنس‌های لازم
        ActorRef<RangeObserverCommand> observer = testKit.spawn(
                RangeObserverActor.create(config)
        );


       // observer.tell(new RangeObserverCommand.StartObserving());

        // شبیه‌سازی دریافت scan code از ScannerActor
        String scannedCode = "abc123";
        observer.tell(new RangeObserverCommand.ScanCode(scannedCode));

        // بررسی اینکه code به receiver فرستاده شده
        String receivedCode = scanResultReceiver.receiveMessage();
        assertEquals(scannedCode, receivedCode);
    }

}
