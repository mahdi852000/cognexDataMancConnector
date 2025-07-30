package org.example.akka.config;

import akka.actor.typed.ActorRef;
import org.example.akka.extra.DataManSystem;
import org.example.akka.message.ScannerCommand;

/**
 * Configuration class for initializing and managing a Range Observer actor.
 * <p>
 * This class holds all the essential parameters and dependencies required
 * to create and configure a Range Observer within the Akka Typed actor system.
 * <p>
 * Fields:
 * - dmcc: Instance of DataManSystem representing the scanner hardware interface.
 * - cmId: Unique identifier for the scanner module.
 * - rangeMin: Minimum range threshold value for detection.
 * - rangeMax: Maximum range threshold value for detection.
 * - rangeOff: Offset value used for range calibration or adjustment.
 * - scannerActor: ActorRef to the ScannerCommand actor for sending commands.
 * - uri: URI identifier for the range observer resource.
 * - host: Hostname or IP address related to the scanner or service.
 * - port: Network port for communication.
 * - scanReceiver: ActorRef to receive scanned string data asynchronously.
 * <p>
 * This immutable configuration object allows for clear separation of configuration
 * from business logic, facilitating easy testing, maintenance, and scalability.
 */

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
