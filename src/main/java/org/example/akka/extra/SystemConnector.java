package org.example.akka.extra;


import org.example.akka.message.Response;

import java.io.IOException;

public interface SystemConnector {

    boolean connected();
    boolean disconnect();
    boolean connect();

    boolean addListener(Listener listener);
    boolean removeListener(Listener listener);


    Response send (Request request) throws IOException;

    public interface Listener {
        void onMessage(Response response);
        void onConnect();
        void onDisconnect();
        void onOccupationChanged(boolean occupied);
    }
}
