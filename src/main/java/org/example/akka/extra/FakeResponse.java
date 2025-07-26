package org.example.akka.extra;
import org.example.akka.message.Response;

/// This class has been created only for Testing Purpose

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
