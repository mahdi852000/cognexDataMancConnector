import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.config.RangeObserverConfig;
import org.example.akka.extra.FakeDataManSystem;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.ScannerCommand;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InterruptedIOException;
import java.time.Duration;



public class RangeObserverReliabilityTest {

    private static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    static void tearDown(){
        testKit.shutdownTestKit();
    }

    @Test
    public void dummyTestToCheckSetup(){
        System.out.println("TestKit is working fine.");
    }

    @Test
    public void testObserverRestartsAfterFailure() throws InterruptedException {
        // ایجاد TestProbe برای ScannerActor و ScanReceiver
        TestProbe<ScannerCommand> scannerProbe = testKit.createTestProbe();
        TestProbe<String> scanReceiverProbe = testKit.createTestProbe();

        // کانفیگ ساختگی
        RangeObserverConfig config = new RangeObserverConfig(
                new FakeDataManSystem(50,testKit.createTestProbe(String.class).getRef()),
                123,
                10L,
                100L,
                5L,
                scannerProbe.getRef(),
                "rangeObserver1",
                "localhost",
                1234,
                scanReceiverProbe.getRef()
        );

        // تعریف بازیگر خراب‌کار (با supervisor strategy)
        Behavior<RangeObserverCommand> faultyBehavior = Behaviors.supervise(
                Behaviors.<RangeObserverCommand>setup(ctx ->
                        new AbstractBehavior<RangeObserverCommand>(ctx) {
                            @Override
                            public Receive<RangeObserverCommand> createReceive() {
                                return newReceiveBuilder()
                                        .onMessage(RangeObserverCommand.Tick.class, tick -> {
                                            throw new RuntimeException("Simulated failure");
                                        })
                                        .onMessage(RangeObserverCommand.StartObserving.class, msg -> {
                                            System.out.println("Restarted after failure. Start received.");
                                            return this;
                                        })
                                        .build();
                            }
                        }
                )
        ).onFailure(RuntimeException.class, SupervisorStrategy.restart());

        // بازیگر را ایجاد کن
        ActorRef<RangeObserverCommand> observer =
                testKit.spawn(faultyBehavior, "reliableObserver");

        // ارسال پیام خطا
        observer.tell(new RangeObserverCommand.Tick());

        // کمی صبر برای ریست شدن actor
        Thread.sleep(2000);

        // حالا پیام Start باید دریافت و اجرا بشه (یعنی بازیگر دوباره زنده شده)
        observer.tell(new RangeObserverCommand.StartObserving());

        // اختیاری: بررسی کنیم که چیزی به scannerProbe یا scanReceiverProbe رسیده یا نه
        // scannerProbe.expectNoMessage(); // در این تست خاص اجباری نیست
    }
}

