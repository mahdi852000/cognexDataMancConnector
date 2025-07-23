import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;

import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.ScannerCommand;
import org.example.akka.message.ScannerCommand.TriggerScan;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import akka.actor.testkit.typed.javadsl.TestProbe;
import org.slf4j.LoggerFactory;


import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangeObserverScannerIntegrationTest {

    public static  final Logger log = LoggerFactory.getLogger(
            RangeObserverScannerIntegrationTest.class);

    static ActorTestKit testKit;

    @BeforeAll
    static void setup(){
        testKit=ActorTestKit.create();
    }


    @AfterAll
    static void cleanup(){
        testKit.shutdownTestKit();
    }

    @Test
    void shouldStopTriggeringScanWhenStopped() throws InterruptedException {
        TestProbe<ScannerCommand> scannerProbe = testKit.createTestProbe();
        TestProbe<String> scanReceiverProbe = testKit.createTestProbe();

        double simulatedDistance = 25.0;
        Duration tickInterval = Duration.ofMillis(5000);

        ActorRef<RangeObserverCommand> observer = testKit.spawn(
                RangeObserverActor.createWithFakeSensor(
                        simulatedDistance,
                        scannerProbe.getRef(),
                        scanReceiverProbe.getRef(),
                        tickInterval
                )
        );

        observer.tell(new RangeObserverCommand.StartObserving());
        Thread.sleep(2100);



        scannerProbe.expectMessageClass(ScannerCommand.SetOccupation.class);

        TriggerScan msg = scannerProbe.expectMessageClass(TriggerScan.class);
        System.out.println("Received TriggerScan: " + msg);




        observer.tell(new RangeObserverCommand.StopObserving());
        scannerProbe.expectNoMessage(Duration.ofMillis(5000));
    }
}

