package org.example.akka.config;

import akka.actor.typed.ActorRef;

import org.example.akka.extra.*;
import org.example.akka.message.*;


public class ScannerActorConfig {
    public final int cmId;
    public final DataManSystem dmcc;
    public final SystemConnector.Listener listener;
    public final IResource delegate;
    public final String host;
    public final int port;
    public final ActorRef<CognexCommand> cognexActor;
    public final boolean isExternalDmcc;
    public final ActorRef<String> scanReceiver;
    public final boolean useCheckSum;
    public final ActorRef<RangeObserverCommand> rangeObserverActor;


    public ScannerActorConfig(
            int cmId,
            DataManSystem dmcc,
            SystemConnector.Listener listener,
            IResource delegate,
            String host,
            int port,
            ActorRef<CognexCommand> cognexActor,
            boolean isExternalDmcc,
            ActorRef<String> scanReceiver,
            boolean useCheckSum,
            ActorRef<RangeObserverCommand> rangeObserverActor
    ) {
        this.cmId = cmId;
        this.dmcc = dmcc;
        this.listener = listener;
        this.delegate = delegate;
        this.host = host;
        this.port = port;
        this.cognexActor = cognexActor;
        this.isExternalDmcc = isExternalDmcc;
        this.scanReceiver = scanReceiver;
        this.useCheckSum = useCheckSum;
        this.rangeObserverActor = rangeObserverActor; // for test
    }

    public ScannerActorConfig(
            int cmId,
            DataManSystem dmcc,
            SystemConnector.Listener listener,
            IResource delegate,
            String host,
            int port,
            ActorRef<CognexCommand> cognexActor,
            boolean isExternalDmcc,
            ActorRef<String> scanReceiver,
            boolean useCheckSum) {
            this(cmId, dmcc, listener, delegate, host, port, cognexActor,
                isExternalDmcc, scanReceiver, useCheckSum, null); // default value
    }


}
