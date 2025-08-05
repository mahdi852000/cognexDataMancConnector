package org.example.akka.extra;

import akka.actor.typed.ActorRef;
import org.example.akka.actor.dmcc.ScannerActor;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.message.Response;

/// This class has been created only for Testing Purpose


public class FakeDataManSystem extends DataManSystem {

    private final long fixedDistance;
    private final ActorRef<String> scanReceiver;


   /* public FakeDataManSystem(double fixedDistance) {
        super(null);
        this.fixedDistance = (long) fixedDistance;
    }*/

    public FakeDataManSystem(double fixedDistance, ActorRef<String> scanReceiver) {
        super(new FakeSystemConnector());  // Pass a valid SystemConnector instance here
        this.fixedDistance = (long) fixedDistance;
        this.scanReceiver=scanReceiver;
    }

    @Override
    public Response sendCommand(String command, Integer id, boolean useCheckSum) {
        return new Response(String.valueOf(fixedDistance),  useCheckSum, id);
    }
    public void simulateScan() {
        String code = "SIMULATED_SCAN_ABC";
        if(scanReceiver != null){
            scanReceiver.tell(code);
        }

    }

}
