package org.example.akka.extra;

import org.example.akka.extra.DataManSystem;
import org.example.akka.message.Response;

public class FakeDataManSystem extends DataManSystem {

    private final long fixedDistance;

    public FakeDataManSystem(double fixedDistance) {
        super(null);
        this.fixedDistance = (long) fixedDistance;
    }

    @Override
    public Response sendCommand(String command, Integer id, boolean useCheckSum) {
        return new Response(String.valueOf(fixedDistance),  useCheckSum, id);
    }
}
