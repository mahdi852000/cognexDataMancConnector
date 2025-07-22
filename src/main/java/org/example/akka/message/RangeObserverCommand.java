package org.example.akka.message;

public interface RangeObserverCommand {
    public record StartObserving () implements RangeObserverCommand{}
    public record StopObserving () implements RangeObserverCommand{}
    public record Tick() implements RangeObserverCommand{}
    public record ScanCode(String code) implements RangeObserverCommand{}
}
