package org.example.akka.message;

import akka.actor.typed.ActorRef;
import org.example.akka.event.SystemEvent;
import org.example.akka.extra.IResource;

public class CognexCommands {
    public interface CognexCommand {}
        public record Start() implements CognexCommand {}
        public record Stop() implements CognexCommand {}
        public record Connect() implements CognexCommand {}
        public record Disconnect() implements CognexCommand {}
        public record SetOccupation(boolean occupied) implements CognexCommand {}
        public record NotifyScannedCode(IResource resource, String code) implements CognexCommand {}


    public record RegisterListener(ActorRef<SystemEvent.CognexEvent> listener) implements CognexCommand {}
        public record UnregisterListener(ActorRef<SystemEvent.CognexEvent> listener) implements CognexCommand {}
}
