🔰 Overview
This project demonstrates how to perform a reliability test on Akka Typed actors in Java, focusing on the interaction between a scanner and a range observer system. The goal is to verify consistent and correct behavior over time without relying on an external TCP mock server.
____________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________

🧱 Project Structure
RangeObserverActor: Monitors environment and triggers scanning periodically.

ScannerActor: Simulates the scanner device’s behavior (real or fake).

FakeDataManSystem: Test double for the scanner system to simulate responses.

ScanReceiver: Test probe actor to receive and validate scan results.

____________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________

🧪 Testing Goals
Verify stable interaction between RangeObserverActor and ScannerActor over repeated cycles.

Ensure that scanner commands and asynchronous scan code responses are handled reliably.

Test timer-based triggers inside RangeObserverActor using Akka’s TimerScheduler.

Remove dependency on external TCP mock servers by using FakeDataManSystem.

____________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________

🛠 Implementation Notes
Implemented in Java using Akka Typed API.

Uses TimerScheduler for periodic triggering in RangeObserverActor.

Employs TestProbe to verify actor messages and interactions.

Supports dependency injection of FakeDataManSystem for test flexibility.


____________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________
 
 Dependencies (Maven)
 <dependency>
  <groupId>com.typesafe.akka</groupId>
  <artifactId>akka-actor-typed_2.13</artifactId>
  <version>2.8.4</version>
</dependency>
<dependency>
  <groupId>com.typesafe.akka</groupId>
  <artifactId>akka-actor-testkit-typed_2.13</artifactId>
  <version>2.8.4</version>
  <scope>test</scope>
</dependency>



