package org.example.akka.testUtils;

public class ScanResultEvent implements ScanEvent{

    private final String result;

    public ScanResultEvent(String result) {
        this.result=result;
    }

    public String getResult() {
        return result;
    }
}
