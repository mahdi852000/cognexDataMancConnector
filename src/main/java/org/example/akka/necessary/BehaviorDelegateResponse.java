package org.example.akka.necessary;

/**
 * Response message carrying the behavior delegate object.
 * This message is sent in reply to a GetBehaviorDelegate request.
 */
public final class BehaviorDelegateResponse {
    public final Object delegate;

    public BehaviorDelegateResponse(Object delegate) {
        this.delegate = delegate;
    }
}
