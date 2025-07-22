package org.example.akka.event;

import org.example.akka.extra.IResource;

public interface SystemEvent {

        interface CognexEvent extends SystemEvent {

        record TriggerSucceeded(String code) implements CognexEvent {}
        record TriggerFailed(String reason) implements CognexEvent {}
            record CodeScanned(IResource resource, String code) implements CognexEvent {}
    }

}
