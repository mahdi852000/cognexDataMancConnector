package org.example.akka.actor.dmcc;

import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.*;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import net.enilink.komma.core.IReference;
import org.example.akka.extra.*;
import org.example.akka.message.*;
import org.example.akka.message.Response;
import org.example.akka.necessary.BehaviorDelegateResponse;
import org.example.akka.necessary.GetBehaviorDelegate;
import org.example.akka.config.RangeObserverConfig;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.extra.TcpConnector;
import org.example.akka.message.RangeObserverCommand;

import org.example.akka.utils.ScannerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public class ScannerActor extends AbstractBehavior<ScannerCommand> implements Behaviour<IResource>, TcpConnector{

    protected static final Logger logger = LoggerFactory.getLogger(ScannerActor.class);

    private final ScannerActorConfig config;
    private int cmId = 0;
    private DataManSystem dmcc;
    boolean heartbeat = false;
    private Boolean occupation = false;
    private SystemConnector.Listener listener;
    private final IResource delegate;

    // Exposed as protected for testing purposes (e.g., to inject mocks or verify state)
    protected ActorRef<RangeObserverCommand> rangeObserverActor;
    protected boolean isRangeObserving = false;

    // Protected for testing purposes (e.g., to mock or verify connection state)
    protected boolean connected = false;
    private Collection<ScannerEventListener> listeners = new CopyOnWriteArrayList<>();

    boolean useCheckSum = false;
    public final ActorRef<CognexCommand> cognexActor;

    private boolean isStarted = false;


    private enum ConnectionState {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
        RECONNECTING
    }

    private ConnectionState connectionState = ConnectionState.DISCONNECTED;
    private int retryCount = 0;
    private static final int MAX_RETRIES = 5;
    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(2);


    public ScannerActor(ActorContext<ScannerCommand> context, ScannerActorConfig config,
                        ActorRef<CognexCommand> cognexActor) {
        super(context);
        this.config=config;
        this.dmcc = config.dmcc;
        this.heartbeat = false;
        this.occupation = null;
        this.listener = config.listener;
        this.delegate=config.delegate;
        this.connected=false;
        this.isRangeObserving=false;
        this.listeners= new CopyOnWriteArrayList<>();
        this.cognexActor = cognexActor;

    }
    public static Behavior<ScannerCommand> create(ScannerActorConfig config)   {
        return Behaviors.setup(ctx->
                new ScannerActor(ctx,config, config.cognexActor));

    }

    private Behavior<ScannerCommand> onGetBehaviorDelegate(GetBehaviorDelegate msg) {
        Object delegate = ((Behaviour<IResource>) this).getBehaviourDelegate();
        msg.replyTo.tell(new BehaviorDelegateResponse(delegate));
        return this;
    }

    /**
     * Registers message handlers related to connection management for the Scanner actor.
     * <p>
     * This method configures the ReceiveBuilder to handle various connection lifecycle events,
     * such as connect, disconnect, and connection status queries.
     *
     * @param builder the ReceiveBuilder to which connection message handlers will be added
     * @return the ReceiveBuilder updated with connection-related handlers
     */

    private ReceiveBuilder<ScannerCommand> connectionHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.Connect.class, this::onConnect)
                .onMessage(ScannerCommand.OnDisconnect.class, this::onOnDisconnect)
                .onMessage(ScannerCommand.OnConnect.class, this::onOnConnect)
                .onMessage(ScannerCommand.IsConnected.class, this::onIsConnected)
                .onMessage(ScannerCommand.IsConnectedToDMCC.class, this::onIsConnectedToDMCC)
                .onMessage(ScannerCommand.QueryIsConnected.class, this::onQueryIsConnected)
                .onMessage(ScannerCommand.Disconnect.class, this::onDisconnect);
    }
    /**
     * Registers message handlers related to scanning operations for the Scanner actor.
     * <p>
     * This method configures the ReceiveBuilder to handle scanning lifecycle commands
     * such as start, stop, triggering scans, and processing incoming scan messages.
     *
     * @param builder the ReceiveBuilder to which scan message handlers will be added
     * @return the ReceiveBuilder updated with scan-related handlers
     */

    private ReceiveBuilder<ScannerCommand> scanHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.Start.class, this::onStart)
                .onMessage(ScannerCommand.Stop.class, this::onStop)
                .onMessage(ScannerCommand.SendTrigger.class, this::onSendTrigger)
                .onMessage(ScannerCommand.TriggerScan.class, this::onTriggerScan)
                .onMessage(ScannerCommand.OnMessage.class, this::onOnMessage);
    }
    /**
     * Registers message handlers for managing event listeners in the Scanner actor.
     * <p>
     * This method configures the ReceiveBuilder to handle registration and
     * unregistration of event listeners that want to receive scanner events.
     *
     * @param builder the ReceiveBuilder to which listener management handlers will be added
     * @return the ReceiveBuilder updated with listener-related handlers
     */

    private ReceiveBuilder<ScannerCommand> listenerHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.RegisterEventListener.class, this::onRegisterEventListener)
                .onMessage(ScannerCommand.UnregisterEventListener.class, this::onUnregisterEventListener);
    }
    /**
     * Registers message handlers for managing occupation status in the Scanner actor.
     * <p>
     * This method configures the ReceiveBuilder to handle commands related to
     * setting, getting, and querying the occupation state of the scanner.
     *
     * @param builder the ReceiveBuilder to which occupation-related handlers will be added
     * @return the ReceiveBuilder updated with occupation management handlers
     */

    private ReceiveBuilder<ScannerCommand> occupationHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.SetOccupation.class, this::onSetOccupation)
                .onMessage(ScannerCommand.GetOccupation.class, this::onGetOccupation)
                .onMessage(ScannerCommand.QueryOccupation.class, this::onQueryOccupation);
    }
    /**
     * Registers miscellaneous message handlers for the Scanner actor.
     * <p>
     * This method adds handlers for less common or auxiliary commands that
     * don't fit into other specific handler groups.
     *
     * @param builder the ReceiveBuilder to which miscellaneous handlers will be added
     * @return the ReceiveBuilder updated with miscellaneous command handlers
     */

    private ReceiveBuilder<ScannerCommand> miscHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.Enqueue.class, this::onEnqueue);
                //.onMessage(GetBehaviorDelegate.class, this::onGetBehaviorDelegate);
    }

    /**
     * Defines the message and signal handlers for this actor.
     * <p>
     * This method aggregates all specific handlers for connection management,
     * scanning operations, listener registration, occupation status, and miscellaneous commands.
     * It also handles the termination signal.
     *
     * @return a Receive instance that handles all registered messages and signals
     */

    @Override
    public Receive<ScannerCommand> createReceive() {
        ReceiveBuilder<ScannerCommand> builder = newReceiveBuilder();
        connectionHandlers(builder);
        scanHandlers(builder);
        listenerHandlers(builder);
        occupationHandlers(builder);
        miscHandlers(builder);

        builder.onSignal(Terminated.class, this::onTerminated);

        return builder.build();
    }


    private Behavior<ScannerCommand>onTriggerScan(ScannerCommand.TriggerScan msg) {
        logger.info("Scanner triggered to scan.");
        // یا انجام عملیات دلخواه
        rangeObserverActor.tell( new RangeObserverCommand.StartObserving());
        return Behaviors.same();
    }


    private Behavior<ScannerCommand> onDisconnect(ScannerCommand.Disconnect msg) {
        if (dmcc != null && dmcc.connected()) {

            if (rangeObserverActor != null && isRangeObserving) {
                rangeObserverActor.tell(new RangeObserverCommand.StopObserving());
                isRangeObserving = false;
            }
            DataManSystem ds = dmcc;
            dmcc = null;
            ds.disconnect(); // بدون try
            ds.removeListener(listener);
            connected = false;
            logger.info("DMCC disconnected and listener removed");
        }
        return this;
    }
    /**
     * Handles QueryOccupation command by replying with the current occupation state.
     * Primarily intended for testing or debugging to verify the actor’s internal occupation status.
     */

    private Behavior<ScannerCommand>onQueryOccupation(ScannerCommand.QueryOccupation msg) {
        msg.replyTo().tell(new ScannerCommand.OccupationStatus(occupation));
        getContext().getLog().info("📥 [ScannerActor] Received QueryOccupation, responding with {}", occupation);
        return this;
    }
    /**
     * Handles GetOccupation command by checking and logging the current occupation status.
     * Intended for debugging or testing purposes to monitor whether the actor is currently occupied.
     */
    private Behavior <ScannerCommand> onGetOccupation (ScannerCommand.GetOccupation msg) {
        getContext().getLog().info("Check being Occupied");
        boolean isOccupied = occupation !=null && occupation;
        getContext().getLog().info("Current occupation status: {}", isOccupied);
        return this;
    }

    private Behavior<ScannerCommand> onSetOccupation(ScannerCommand.SetOccupation msg) {
        boolean occupied = msg.occupied();
        if(null==occupation || occupied!=occupation ){
            occupation = occupied;
        }
        return this;
    }
    /**
     * Handles the OnDisconnect event, typically triggered after an unexpected disconnection.
     *
     * - Stops range observation if active.
     * - Disconnects from the DMCC system if currently connected.
     * - Updates the internal state to RECONNECTING and initiates a scheduled reconnect attempt.
     *
     * @param msg the OnDisconnect message
     * @return the current actor behavior
     */
    private Behavior<ScannerCommand> onOnDisconnect(ScannerCommand.OnDisconnect msg) {
        getContext().getLog().info("Handling OnDisconnect. Current state: {}", connectionState);

        if (rangeObserverActor != null && isRangeObserving) {
            rangeObserverActor.tell(new RangeObserverCommand.StopObserving());
            isRangeObserving = false;
        }
        if (dmcc != null && dmcc.connected()) {
            dmcc.disconnect();
            connected = false;
            getContext().getLog().info("Disconnected from DMCC.");
        }
        connectionState = ConnectionState.RECONNECTING;
        retryCount = 1;
        scheduleReconnect();

        return this;
    }


    /**
     * Handles the Connect command to initiate a connection to the DMCC system.
     * <p>
     * - Skips the process if already connected.
     * - Updates internal state to CONNECTING and attempts to connect.
     * - If successful:
     *     - Updates state to CONNECTED.
     *     - Notifies the Cognex actor to initiate its own connection.
     * - If unsuccessful:
     *     - Sets state to RECONNECTING.
     *     - Schedules a retry attempt with exponential backoff logic.
     *
     * @param msg the Connect command message
     * @return the current actor behavior
     */

    private Behavior<ScannerCommand> onConnect (ScannerCommand.Connect msg) {

        getContext().getLog().info("onConnect() called. Current state: {}", connectionState);

        if(connectionState == ConnectionState.CONNECTED) {
            getContext().getLog().info("Already connected. Ignoring connect request.");
            return this;
        }
        connectionState = ConnectionState.CONNECTING;
        retryCount=0;
        getContext().getLog().info("Attempting to connect...");

        try {
            dmcc.connect();
            if (dmcc.connected()) {
                connectionState = ConnectionState.CONNECTED;
                this.connected = true;
                getContext().getLog().info("Connected successfully.");
                cognexActor.tell(new CognexCommand.Connect());

            } else {
                connectionState = ConnectionState.RECONNECTING;
                retryCount = 1;
                getContext().getLog().warn("Initial connection failed. Will retry...");
                scheduleReconnect();
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * Placeholder for handling OnMessage command.
     * Currently, logs a message for debugging; not yet integrated or used in the system.
     * Can be extended in the future if message-based processing is required.
     */
    // TODO: Evaluate if OnMessage is needed — currently unused and only logs a placeholder.
    @Deprecated
    private Behavior<ScannerCommand> onOnMessage (ScannerCommand.OnMessage msg) {
        Response response = msg.response();
        getContext().getLog().info("OUR APPROPRIATE MESSAGE ");
        return Behaviors.same();
    }

    /**
     * Handles Enqueue command by receiving a DTO. Currently, it doesn't process or respond to the message.
     * The replyTo field is acknowledged but unused — can be used in future implementations if needed.
     */
    private Behavior<ScannerCommand> onEnqueue (ScannerCommand.Enqueue msg) {
        IDTO dto = msg.dto();
        ActorRef<Boolean> replyTo = msg.replyTo();// Currently unused, reserved for future acknowledgment
        return Behaviors.same();
    }

    private Behavior<ScannerCommand> onIsConnectedToDMCC (ScannerCommand.IsConnectedToDMCC msg){
        this.connected=msg.value();
        if(connected){
            getContext().getLog().info(" DMCC connection is OK.");
        } else  {
            getContext().getLog().info(" DMCC connection FAILED.");
        }
        return this;
    }

    private Behavior<ScannerCommand> onIsConnected (ScannerCommand.IsConnected msg) {
        this.connected = msg.value();
        if(connected){
            logger.info("Connection is Ok");
        } else {
            logger.warn("Connection failed");
        }
        // Handling connection status
        return this;
    }
    private Behavior<ScannerCommand> onRegisterEventListener (ScannerCommand.RegisterEventListener msg) {
        ScannerCommand.ScannerEventListener listener = msg.listener();
        if(null!=listener){
            listeners.add((ScannerEventListener) listener);
            getContext().getLog().info("Listener registered: {}", listener);
        }
        return this;
    }
    private Behavior<ScannerCommand> onUnregisterEventListener (ScannerCommand.UnregisterEventListener msg) {

        ScannerCommand.ScannerEventListener listener = msg.listener();
        if (listener != null) {
            listeners.remove(listener); // معادل نسخه کلاسیک
            getContext().getLog().info("Listener unregistered: {}", listener);
        }
        return this;
    }

    private Behavior<ScannerCommand> onTerminated(Terminated sig) {
        if (sig.getRef().equals(rangeObserverActor)) {
            logger.warn("rangeObserverActor is terminated!");
            rangeObserverActor = null;
            isRangeObserving = false;
        }
        return this;
    }
    private Behavior<ScannerCommand> onStop (ScannerCommand.Stop msg) {
        getContext().getLog().info("This is supposed to stop the connector");
        if(rangeObserverActor !=null && isRangeObserving) {
            rangeObserverActor.tell(new RangeObserverCommand.StopObserving());
            isRangeObserving=false;
            getContext().getLog().info("RangeObserverActor stopped");
        }
        return Behaviors.same();
    }
    private Behavior<ScannerCommand> onSendTrigger (ScannerCommand.SendTrigger msg) {
        if(connected){
            getContext().getLog().info("DMCC is connected, sending trigger...");
        } else {
            getContext().getLog().info("DMCC is NOT connected. Trigger skipped.");
        }
        return this;
    }
    private Behavior<ScannerCommand> onOnConnect (ScannerCommand.OnConnect msg) {
        if(dmcc.connected()) return this;
        getContext().getLog().info("DMCC is connected");

        String uri =((IReference) getBehaviourDelegate()).getURI().toString();
        heartbeat = Boolean.TRUE.equals(org.example.akka.utils.ScannerUtils.getProperty
                (delegate,Boolean.class , "heartbeat"));
       // TcpSystemConnector conn = new TcpSystemConnector(host(),port()).useHeartBeat(heartbeat);

        if(!dmcc.connected()) {
                if(config.isExternalDmcc){
                    getContext().getLog().info("External DMCC injected, skipping override.");
                } else {
                    TcpSystemConnector conn = new TcpSystemConnector(host(), port()).useHeartBeat(heartbeat);
                    dmcc = new DataManSystem(conn);
                    getContext().getLog().info("dmcc instance is: {}", dmcc.getClass());
                }
            }

        listener = new SystemConnector.Listener() {
            @Override
            public void onMessage(Response response) {
                String code = response.result();
                logger.info("gateway-scan got code={} at source{}", code, uri);
                config.cognexActor.tell(new CognexCommand.NotifyScannedCode(getBehaviourDelegate(),code));
                listeners.forEach(l ->
                        l.onCodeScanned(getBehaviourDelegate(),code));
            }


            @Override
            public void onConnect() {
                try {
                    logger.info("gateway-scan connected source={}", uri);
                    dmcc.sendCommand("SET COM.DMCC-RESPONSE 1", cmId++, useCheckSum );
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }

            @Override
            public void onDisconnect() {
                logger.info("gateway-scan disconnected source={}", uri);
                if(null!=rangeObserverActor) {
                    rangeObserverActor.tell(new RangeObserverCommand.StopObserving());
                    isRangeObserving=false;
                    logger.info("Sent StopObserving to RangeObserverActor");
                };
                if(null!=dmcc){
                    dmcc.connect();
                }
            }
        };
        dmcc.addListener(listener);
        dmcc.connect();
        this.connected=true;

        return this;
    }
    private Behavior<ScannerCommand> onQueryIsConnected(ScannerCommand.QueryIsConnected msg) {
        boolean status = dmcc != null && dmcc.connected(); // همون منطق متد قبلی isConnected()
        msg.replyTo().tell(new ScannerCommand.ConnectedStatus(this.connected));
        return this;
    }

    private Behavior<ScannerCommand> onStart(ScannerCommand.Start msg) {

        if (isStarted) {
            logger.info("Scanner already started, ignoring duplicate Start command.");
            return this;
        }

        logger.info("Starting " + getBehaviourDelegate());

        IReference ref = (getBehaviourDelegate() instanceof IReference) ? (IReference) getBehaviourDelegate() : null;
        String uri = (ref != null && ref.getURI() != null) ? ref.getURI().toString() : "UNKNOWN";

        logger.info("Starting DMCC scanner at URI= {} host = {} port= {}", uri, host(), port());
        logger.info("Starting observation for scanner URI = {}", uri);

        try {
            if (dmcc == null || !dmcc.connected()) {
                logger.warn("DMCC is not connected, attempting to connect...");
                getContext().getSelf().tell(new ScannerCommand.Connect());
                return this;
            }

            Response r = dmcc.sendCommand("UPTIME");
            if (r instanceof Response.NoResponse) {
                logger.warn("No response from UPTIME, triggering disconnect and reconnect.");
                getContext().getSelf().tell(new ScannerCommand.OnDisconnect());
                getContext().getSelf().tell(new ScannerCommand.Connect());
                return this;
            }
            logger.info("UPTIME response = {}", r);

            Optional<Long> rangeMax = ScannerUtils.getProperty(delegate, Long.class, "triggerRangeMax");
            if (rangeMax.isPresent() && rangeObserverActor != null && !isRangeObserving) {
                rangeObserverActor.tell(new RangeObserverCommand.StartObserving());
                isRangeObserving = true;
                logger.info("Started RangeObserver for range max: {}", rangeMax.get());
            }
            // ✅ فقط در صورتی که همه چیز موفق بود:
            isStarted = true;

        } catch (IOException e) {
            logger.error("Error during onStart: ", e);
            getContext().getSelf().tell(new ScannerCommand.OnDisconnect());
            // isStarted را نمی‌گذاریم true بماند چون شروع موفق نبود
        }

        return this;
    }
    /**
     * Schedules a reconnect attempt with a delay defined by RETRY_INTERVAL.
     * <p>
     * If the number of retry attempts exceeds MAX_RETRIES, it logs a warning,
     * sets the connection state to DISCONNECTED, and stops further retries.
     * Otherwise, it schedules a reconnect command to be sent to self after the delay.
     * <p>
     * Also increments the retry count after each scheduled attempt.
     */

    private void scheduleReconnect() {
        if(retryCount>MAX_RETRIES) {
            getContext().getLog().warn("Max reconnect attempts reached. Switching to DISCONNECTED.");
            connectionState=ConnectionState.DISCONNECTED;
            return;
        }
        getContext().getSystem().scheduler().scheduleOnce(
                RETRY_INTERVAL,
                ()->getContext().getSelf().tell(new ScannerCommand.Connect()),
                        getContext().getSystem().executionContext());
        getContext().getLog().info("Scheduled reconnect attempt {} after {} seconds",
                retryCount, RETRY_INTERVAL.getSeconds());
                retryCount++;
    }
@Override
public IResource getBehaviourDelegate() {
    return this.delegate;
}

    @Override
    public String host() {
        return this.config.host;
    }

    @Override
    public int port() {
        return this.config.port;
    }
}
