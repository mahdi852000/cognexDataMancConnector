package org.example.akka.extra;

import net.enilink.composition.annotations.Iri;

public interface TcpConnector {
    @Iri(CONNECTORS.CONNECTOR_HOST)
    public String host();

    @Iri(CONNECTORS.CONNECTOR_PORT)
    public int port();

}
