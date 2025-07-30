🔰 Overview
This report presents the reliability testing of an Akka Typed actor in Java, specifically focusing on the RangeObserverActor. The aim is to ensure that this actor responds correctly and consistently to repeated control messages, such as Start, Stop, and Tick, under test conditions.
______________________________________________________________________________________________________________________________________________________________________________________________________

🧱 System Components
RangeObserverActor: Reacts to environmental triggers using a TimerScheduler, sending simulated scan codes to a predefined receiver.

FakeDataManSystem: A test double used only to construct the actor under test.

ScanReceiver (TestProbe<String>): A probe that receives messages and validates their content

______________________________________________________________________________________________________________________________________________________________________________________________________

🧪 Testing Objectives
Validate that RangeObserverActor handles Start, Stop, and Tick messages in a reliable and repeatable way.

Confirm that timer-based triggering produces the expected number of scan code messages.

Verify that scan messages are only sent when the actor is in the correct running state.

Ensure that stopping the actor halts further message production.

______________________________________________________________________________________________________________________________________________________________________________________________________

🛠 Implementation Details
Implemented in Java using Akka Typed API (version 2.8.4).

Used Akka’s TimerScheduler for periodic execution inside the actor.

TestProbe<String> verifies that the expected scan messages are received.

The FakeDataManSystem is used to instantiate the actor but plays no active role in logic during this test.


______________________________________________________________________________________________________________________________________________________________________________________________________


🔍 Challenges
Timer control in tests: Ensuring timers fired predictably and did not overlap with stop commands was a key challenge.

Clean shutdown behavior: Verifying that the actor stopped emitting scan codes immediately after receiving a Stop message required precise timing.

State validation: Making sure that scan messages were only sent in the correct running state (after Start) and ignored in the stopped state.


______________________________________________________________________________________________________________________________________________________________________________________________________


📊 Results
The RangeObserverActor reliably produced scan messages after Start and stopped producing them after receiving Stop.

No scan messages were received while the actor was in the stopped state.

The actor behaved deterministically during multiple start-stop cycles, proving its reliability under repeated command sequences.

______________________________________________________________________________________________________________________________________________________________________________________________________

💡 Future work

Add edge-case tests: Include more scenarios such as sending Start multiple times or sending Stop before any Start.

Parameterize scan intervals: Make timer delay configurable for better test control.

Consider state introspection: Add logging or state inspection tools to simplify actor testing in complex systems.

Test under load: Evaluate the actor’s behavior under high-frequency tick simulations to test performance limits.

______________________________________________________________________________________________________________________________________________________________________________________________________
 
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

______________________________________________________________________________________________________________________________________________________________________________________________________

