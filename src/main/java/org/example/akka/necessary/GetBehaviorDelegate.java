package org.example.akka.necessary;

import akka.actor.typed.ActorRef;
import org.example.akka.message.ScannerCommand;

public class GetBehaviorDelegate implements ScannerCommand {
    public final ActorRef<BehaviorDelegateResponse> replyTo;

    public GetBehaviorDelegate(ActorRef<BehaviorDelegateResponse> replyTo) {
        this.replyTo = replyTo;
    }
}
