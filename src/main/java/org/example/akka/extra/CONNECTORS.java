package org.example.akka.extra;

import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;

public interface CONNECTORS {
    public static final String NAMESPACE = "http://Linkedfactory";
    public static final URI NAMESPACE_URI = URIs.createURI(NAMESPACE);


    ////////////////////////////////////////////////////////////////
    public static final String CONNECTOR_HOST = NAMESPACE + "host";
    public static final URI PROPERTY_CONNECTOR_IP = URIs.createURI(CONNECTOR_HOST);


    public static final String CONNECTOR_PORT = NAMESPACE + "port";
    public static final URI PROPERTY_CONNECTOR_PORT = URIs.createURI(CONNECTOR_PORT);

    public static final String CONNECTOR= NAMESPACE + "Connector";
    public static final URI TYPE_CONNECTOR = URIs.createURI(CONNECTOR);

    public static final String CONNECTOR_AUTOCONNECT = NAMESPACE + "autoConnect";
    public static final URI PROPERTY_CONNECTOR_AUTOCONNECT = URIs.createURI(CONNECTOR_AUTOCONNECT);

    public static final String CONNECTOR_NAME = NAMESPACE + "name";
    public static final URI PROPERTY_CONNECTOR_NAME = URIs.createURI(CONNECTOR_NAME);

}