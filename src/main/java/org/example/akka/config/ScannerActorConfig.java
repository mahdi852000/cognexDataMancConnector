package org.example.akka.config;

import akka.actor.typed.ActorRef;

import org.example.akka.extra.*;
import org.example.akka.message.*;

/**
 * Configuration class for initializing and setting up a Scanner Actor.
 * <p>
 * This class encapsulates all necessary parameters and dependencies required
 * to create and operate a scanner actor within the Akka Typed actor system.
 * <p>
 * Fields:
 * - cmId: Unique identifier for the scanner module instance.
 * - dmcc: DataManSystem instance representing the scanner hardware interface.
 * - listener: Callback listener to receive system connector events.
 * - delegate: IResource representing the resource delegate associated with this scanner.
 * - host: Network hostname or IP address of the scanner or related service.
 * - port: Network port number for communication with the scanner.
 * - cognexActor: Reference to the CognexCommand actor for sending commands to the scanner.
 * - isExternalDmcc: Flag indicating if the DataManSystem instance is external to this actor.
 * - scanReceiver: ActorRef for receiving scanned data strings asynchronously.
 * - useCheckSum: Flag indicating whether checksum validation is enabled for data integrity.
 * <p>
 * This immutable configuration object is intended to be passed into actor constructors or factories,
 * enabling clear separation of configuration from business logic.
 */


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
            boolean useCheckSum
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
    }
}
