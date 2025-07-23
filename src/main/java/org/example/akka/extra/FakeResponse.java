package org.example.akka.extra;

import org.example.akka.message.Response;

public class FakeResponse extends Response {
    private final String result;

    public FakeResponse(String result) {
        this.result = result;
    }

    @Override
    public String result() {
        return result;
    }
}
