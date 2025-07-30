package org.example.akka.actor.dmcc;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import static org.example.akka.message.CognexCommand.*;
import org.example.akka.message.CognexCommand;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import org.example.akka.event.SystemEvent;

import java.util.HashSet;
import java.util.Set;


public class CognexDataManActor extends AbstractBehavior<CognexCommand> {

    private final Set<ActorRef<SystemEvent.CognexEvent>> listeners = new HashSet<>();

    private CognexDataManActor(ActorContext<CognexCommand> context) {
        super(context);
    }

    public static Behavior<CognexCommand> create() {
        return Behaviors.setup(CognexDataManActor::new);
    }

    @Override
    public Receive<CognexCommand> createReceive() {
        return newReceiveBuilder()
                /*.onMessage(Start.class,this::onStart)
                .onMessage(Stop.class, this::onStop)
                .onMessage(Connect.class, this::onConnect)
                .onMessage(Disconnect.class, this::onDisconnect)
                .onMessage(SetOccupation.class, this::onSetOccupation)
                .onMessage(RegisterListener.class, this::onRegisterListener)
                .onMessage(UnregisterListener.class, this::onUnregisterListener)*/
                .onMessage(NotifyScannedCode.class, this::onNotifyScannedCode)
                .build();
    }

    /**
     * === Future Commands Placeholder ===
     * The following command handlers are currently unused, but are kept here
     * for potential future implementation when actor behavior needs to handle
     * start/stop/connect/disconnect/occupation and listener registration commands.
     * <p>
     * Uncomment and implement as needed.
     */

    // Handles a Start command
 /*   private Behavior<CognexCommand> onStart(Start msg) {
        getContext().getLog().info("Received Start command");
        return this;
    }
    // Handles a Stop command
    private Behavior<CognexCommand> onStop(Stop msg) {
        getContext().getLog().info("Received Stop command");
        return this;
    }
    // Handles a Connect command
    private Behavior<CognexCommand> onConnect(Connect msg) {
        getContext().getLog().info("Received Connect command");
        return this;
    }
    // Handles a Disconnect command
    private Behavior<CognexCommand> onDisconnect(Disconnect msg) {
        getContext().getLog().info("Received Disconnect command");
        return this;
    }
    // Handles setting occupation status
    private Behavior<CognexCommand> onSetOccupation(SetOccupation msg) {
        getContext().getLog().info("Received SetOccupation command with value: " + msg.occupied());
        return this;
    }
    // Registers a new listener
    private Behavior<CognexCommand> onRegisterListener(CognexCommands.RegisterListener msg) {
        listeners.add(msg.listener());
        return this;
    }
    // Unregisters an existing listener
    private Behavior<CognexCommand> onUnregisterListener(CognexCommands.UnregisterListener msg) {
        listeners.remove(msg.listener());
        return this;
    }*/

    /**
     * Handles a scanned code notification from the scanner.
     * <p>
     * This method receives a NotifyScannedCode message containing the scanned code
     * and the scanner's resource ID. It then notifies all registered listeners
     * by sending them a CodeScanned event.
     * <p>
     * This is the only method in this actor that processes scanned codes.
     *
     * @param msg the scanned code notification containing the resource and code
     * @return the unchanged behavior
     */

    private Behavior<CognexCommand> onNotifyScannedCode(CognexCommand.NotifyScannedCode msg) {
        for(ActorRef<SystemEvent.CognexEvent> listener:listeners) {
            listener.tell(new SystemEvent.CognexEvent.CodeScanned(msg.resource(),msg.code()));
        }
        return this;
    }

}
