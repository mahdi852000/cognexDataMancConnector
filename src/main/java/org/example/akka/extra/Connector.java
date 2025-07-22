package org.example.akka.extra;

import net.enilink.composition.annotations.Iri;
import org.example.akka.message.IDTO;

import java.io.IOException;


@Iri(CONNECTORS.CONNECTOR)
public interface Connector {

    @Iri(CONNECTORS.CONNECTOR_NAME)
    public String name();

    @Iri(CONNECTORS.CONNECTOR_AUTOCONNECT)
    public boolean autoConnect();


    public void connect() throws IOException;
    public boolean isConnected();
    public void disconnect() throws IOException;
    public boolean enqueue(IDTO dto);


}
