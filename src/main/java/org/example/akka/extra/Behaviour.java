package org.example.akka.extra;

public interface Behaviour<T>{
    String GET_ENTITY_METHOD = "getBehaviourDelegate";

    T getBehaviourDelegate ();
}
