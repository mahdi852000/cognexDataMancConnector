package org.example.akka.actor.dmcc;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import org.example.akka.extra.DataManSystem;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.Response;
import org.example.akka.message.ScannerCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class RangeObserverActor extends AbstractBehavior<RangeObserverCommand> {



    private final TimerScheduler<RangeObserverCommand> timers;
    private final DataManSystem dmcc;
    private int cmId;
    private final Long rangeMin, rangeMax, rangeOff;
    private final long[] measurements = new long[5];
    private int pos = 0;
    private final ActorRef<ScannerCommand> scannerActor;
    private Boolean occupation = null;
    private final  String uri;
    private final String host;
    private final int port;

    private static final Object TICK_KEY = new Object();
    private final ActorRef<String> scanReceiver;




    private RangeObserverActor(
            ActorContext<RangeObserverCommand> context ,
            TimerScheduler<RangeObserverCommand> timers,
            DataManSystem dmcc,
            int cmId,
            Long rangeMin,
            Long rangeMax,
            Long rangeOff,
            ActorRef<ScannerCommand> scannerActor,
            String uri,
            String host,
            int port,
            ActorRef<String> scanReceiver)
    {
        super(context);
        this.timers= timers;
        this.dmcc = dmcc;
        this.cmId = cmId;
        this.rangeMin = rangeMin;
        this.rangeMax = rangeMax;
        this.rangeOff = rangeOff;
        this.scannerActor = scannerActor;
        this.host=host;
        this.uri=uri;
        this.port=port;
        this.scanReceiver=scanReceiver;

    }
    public static Behavior<RangeObserverCommand> create(DataManSystem dmcc, int cmId
    , Long rangeMin
    , Long rangeMax
    , Long rangeOff
    , ActorRef<ScannerCommand> scannerActorRef,String uri, String host, int port,ActorRef<String> scanReceiver) {
        return Behaviors.withTimers(timers->
                Behaviors.setup(
                        ctx-> new RangeObserverActor(ctx, timers,dmcc,cmId,rangeMin
                        ,rangeMax,rangeOff,scannerActorRef,uri,host,port,scanReceiver)));
    }


    @Override
    public  Receive<RangeObserverCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(RangeObserverCommand.StartObserving.class, this::onStartObservingRange)
                .onMessage(RangeObserverCommand.StopObserving.class, this::onStopObservingRange)
                .onMessage(RangeObserverCommand.Tick.class, this::onTick)
                .onMessage(RangeObserverCommand.ScanCode.class, this::onScanCode)
                .build();
    }
    private Behavior<RangeObserverCommand> onScanCode(RangeObserverCommand.ScanCode msg) {
        scanReceiver.tell(msg.code());
        getContext().getLog().info("Received scan code: {}",msg.code() );
        return this;
    }

    private Behavior<RangeObserverCommand> onStartObservingRange(RangeObserverCommand.StartObserving startObserving) {
        timers.startTimerAtFixedRate(TICK_KEY, new RangeObserverCommand.Tick(), Duration.ofSeconds(5));
        getContext().getLog().info("Range Observing Started");
        return this;
    }



    private Behavior<RangeObserverCommand> onStopObservingRange(RangeObserverCommand.StopObserving stopObserving) {
        timers.cancel(TICK_KEY);
        getContext().getLog().info("Range Observing Stopped");
        return this;
    }

    private Behavior<RangeObserverCommand> onTick(RangeObserverCommand.Tick tick) {
        try {
            Response r = dmcc.sendCommand("GET HEIGHT-SENSOR.CURRENT-MEASUREMENT", cmId++, true);

            if (r == null) {
                getContext().getLog().warn("Received null Response from DMCC");
                return this;
            }

           if (r.result() == null) {
                getContext().getLog().warn("Null result from DMCC");
                return this;
            }

            long measurement = Long.parseLong(r.result());
            measurements[pos] = measurement;
            pos = (pos + 1) % measurements.length;

            double avg = java.util.Arrays.stream(measurements)
                    .filter(m -> m > 0)
                    .average()
                    .orElse(0.0);

            getContext().getLog().info("Avg(5)={}, measurement={}", avg, measurement);

            if (rangeMin < avg && avg < rangeMax) {
                if (occupation == null || !occupation) {
                    occupation = true;
                    scannerActor.tell(new ScannerCommand.SetOccupation(true));

                    scanReceiver.tell(String.valueOf(measurement));


                    scannerActor.tell(new ScannerCommand.TriggerScan()); //This is my Question! is this what we want?

                    getContext().getLog().info("Occupation changed to ON");
                }
            } else if (avg > rangeOff) {
                if (occupation == null || occupation) {
                    occupation = false;
                    scannerActor.tell(new ScannerCommand.SetOccupation(false));
                    getContext().getLog().info("Occupation changed to OFF");
                }
            }

        } catch (Throwable t) {
            getContext().getLog().error("Error during range observation: {}", t.getMessage(), t);
        }

        getContext().getLog().info("Starting DMCC scanner at URI={} host={} port={}", uri, host, port);

        return this;
    }

}
