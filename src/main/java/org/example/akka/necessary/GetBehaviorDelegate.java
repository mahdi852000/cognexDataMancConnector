package org.example.akka.necessary;

import akka.actor.typed.ActorRef;
import org.example.akka.message.ScannerCommand;

/**
 * Message to request the current behavior delegate from a Scanner actor.
 * The sender provides a reference (replyTo) to receive the BehaviorDelegateResponse.
 */
public class GetBehaviorDelegate implements ScannerCommand {
    public final ActorRef<BehaviorDelegateResponse> replyTo;

    public GetBehaviorDelegate(ActorRef<BehaviorDelegateResponse> replyTo) {
        this.replyTo = replyTo;
    }
}
