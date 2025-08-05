package org.example.akka.extra;

import org.example.akka.message.Response;

import java.io.IOException;

public class FakeSystemConnector implements SystemConnector{
    @Override
    public boolean connect() {
        System.out.println("is connecting");
        return true;
    }

    @Override
    public boolean addListener(Listener listener) {
        return false;
    }

    @Override
    public boolean removeListener(Listener listener) {
        return false;
    }

    @Override
    public Response send(Request request) throws IOException {
        return null;
    }

    @Override
    public boolean connected() {
        return false;
    }

    @Override
    public boolean disconnect() {
        System.out.println("is disconnecting");
        return false;
    }
}
