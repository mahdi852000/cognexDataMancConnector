package org.example.akka.config;

import akka.actor.typed.ActorRef;
import org.example.akka.extra.DataManSystem;
import org.example.akka.message.ScannerCommand;

public class RangeObserverConfig {
    public final DataManSystem dmcc;
    public final int cmId;
    public final Long rangeMin, rangeMax, rangeOff;
    public final ActorRef<ScannerCommand> scannerActor;
    public final String uri, host;
    public final int port;
    public final ActorRef<String> scanReceiver;

    public RangeObserverConfig(
            DataManSystem dmcc,
            int cmId,
            Long rangeMin,
            Long rangeMax,
            Long rangeOff,
            ActorRef<ScannerCommand> scannerActor,
            String uri,
            String host,
            int port,
            ActorRef<String> scanReceiver
    ) {
        this.dmcc = dmcc;
        this.cmId = cmId;
        this.rangeMin = rangeMin;
        this.rangeMax = rangeMax;
        this.rangeOff = rangeOff;
        this.scannerActor = scannerActor;
        this.uri = uri;
        this.host = host;
        this.port = port;
        this.scanReceiver = scanReceiver;
    }
}
