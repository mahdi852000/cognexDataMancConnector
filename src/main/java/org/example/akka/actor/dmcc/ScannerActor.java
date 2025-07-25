package org.example.akka.actor.dmcc;

import akka.actor.typed.Terminated;

import akka.actor.typed.javadsl.*;
import org.example.akka.config.RangeObserverConfig;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.message.RangeObserverCommand;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import net.enilink.komma.core.IReference;
import org.example.akka.extra.*;
import org.example.akka.message.*;
import org.example.akka.message.Response;
import org.example.akka.necessary.BehaviorDelegateResponse;
import org.example.akka.necessary.GetBehaviorDelegate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.example.akka.extra.TcpConnector;

import org.example.akka.util.ScannerUtils;


import java.io.IOException;
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

    private ActorRef<RangeObserverCommand> rangeObserverActor;
    private boolean isRangeObserving = false;

    private boolean connected = false;
    private Collection<ScannerEventListener> listeners = new CopyOnWriteArrayList<>();

    boolean useCheckSum = false;


    public ScannerActor(ActorContext<ScannerCommand> context, ScannerActorConfig config) {
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

    }
    public static Behavior<ScannerCommand> create(ScannerActorConfig config)   {
        return Behaviors.setup(ctx->
                new ScannerActor(ctx,config));

    }

    private Behavior<ScannerCommand> onGetBehaviorDelegate(GetBehaviorDelegate msg) {
        Object delegate = ((Behaviour<IResource>) this).getBehaviourDelegate(); // اگه this کلاس ScannerActor بود
        msg.replyTo.tell(new BehaviorDelegateResponse(delegate));
        return this;
    }

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

    private ReceiveBuilder<ScannerCommand> scanHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.Start.class, this::onStart)
                .onMessage(ScannerCommand.Stop.class, this::onStop)
                .onMessage(ScannerCommand.SendTrigger.class, this::onSendTrigger)
                .onMessage(ScannerCommand.TriggerScan.class, this::onTriggerScan)
                .onMessage(ScannerCommand.OnMessage.class, this::onOnMessage);
    }

    private ReceiveBuilder<ScannerCommand> listenerHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.RegisterEventListener.class, this::onRegisterEventListener)
                .onMessage(ScannerCommand.UnregisterEventListener.class, this::onUnregisterEventListener);
    }

    private ReceiveBuilder<ScannerCommand> occupationHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.SetOccupation.class, this::onSetOccupation)
                .onMessage(ScannerCommand.GetOccupation.class, this::onGetOccupation)
                .onMessage(ScannerCommand.QueryOccupation.class, this::onQueryOccupation);
    }

    private ReceiveBuilder<ScannerCommand> miscHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.Enqueue.class, this::onEnqueue)
                .onMessage(GetBehaviorDelegate.class, this::onGetBehaviorDelegate);
    }
   // @Override
  //  public Receive<ScannerCommand> createReceive2() {
     //   return newReceiveBuilder()
                //.onMessage(ScannerCommand.Connect.class, this::onConnect)
                //.onMessage(ScannerCommand.OnDisconnect.class, this::onOnDisconnect)
                //.onMessage(ScannerCommand.OnMessage.class, this::onOnMessage)
                //.onMessage(ScannerCommand.Enqueue.class, this::onEnqueue)
                //.onMessage(ScannerCommand.IsConnected.class, this::onIsConnected)
                //.onMessage(ScannerCommand.IsConnectedToDMCC.class, this::onIsConnectedToDMCC)
                //.onMessage(ScannerCommand.RegisterEventListener.class, this::onRegisterEventListener)
                //.onMessage(ScannerCommand.UnregisterEventListener.class, this::onUnregisterEventListener)
                //.onMessage(ScannerCommand.Start.class, this::onStart)
                //.onMessage(ScannerCommand.Stop.class, this::onStop)
                //.onMessage(ScannerCommand.SetOccupation.class, this::onSetOccupation)
                //.onMessage(ScannerCommand.GetOccupation.class, this::onGetOccupation)
                //.onMessage(ScannerCommand.SendTrigger.class, this::onSendTrigger)
                //.onMessage(ScannerCommand.OnConnect.class, this::onOnConnect)
                //.onMessage(GetBehaviorDelegate.class, this::onGetBehaviorDelegate)
                //.onSignal(Terminated.class, this::onTerminated)
                //.onMessage(ScannerCommand.QueryIsConnected.class, this::onQueryIsConnected)
                //.onMessage(ScannerCommand.Disconnect.class, this::onDisconnect)
                //.onMessage(ScannerCommand.QueryOccupation.class, this::onQueryOccupation)
                //.onMessage(ScannerCommand.TriggerScan.class, this::onTriggerScan)
       //         .build();
   // }

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

    private Behavior<ScannerCommand>onQueryOccupation(ScannerCommand.QueryOccupation msg) {
        msg.replyTo().tell(new ScannerCommand.OccupationStatus(occupation)); // This has been created for TEST purpose
        return this;
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

    private Behavior<ScannerCommand> onOnDisconnect (ScannerCommand.OnDisconnect msg) {

        getContext().getLog().info("Handling OnDisconnect...");
        connected=false;
        return this;
    }

    private Behavior<ScannerCommand> onConnect (ScannerCommand.Connect msg) {
        getContext().getLog().info("is connecting");
        getContext().getLog().info("onConnect Called");
        dmcc.connect();
        if(dmcc.connect()) {
            this.connected=true;
            getContext().getLog().info("Hey DMCC is connected");
        } else {
            getContext().getLog().warn("DMCCC NOT connected");
        }
        //this.connected = true;
        return this;
    }

    private Behavior<ScannerCommand> onOnMessage (ScannerCommand.OnMessage msg) {
        Response response = msg.response();
        getContext().getLog().info("some Messages 2");
        return Behaviors.same();
    }
    private Behavior<ScannerCommand> onEnqueue (ScannerCommand.Enqueue msg) {
        IDTO dto = msg.dto();
        ActorRef<Boolean> replyTo = msg.replyTo();   //(What is supposed to do, apparently nothing here)
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
        // پردازش وضعیت اتصال
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
            logger.info("RangeObserverActor stopped");
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
        heartbeat = Boolean.TRUE.equals(org.example.akka.util.ScannerUtils.getProperty(delegate,Boolean.class , "heartbeat"));
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
                config.cognexActor.tell(new CognexCommands.NotifyScannedCode(getBehaviourDelegate(),code));
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

        logger.info("Starting " + getBehaviourDelegate());

                //((IReference) getBehaviourDelegate()).getURI().toString();
        IReference ref = ((IReference) getBehaviourDelegate() instanceof IReference) ? (IReference)
        getBehaviourDelegate() : null;
        String uri = (ref != null && ref.getURI() != null) ? ref.getURI().toString() : "UNKNOWN";

        /*String host = host();
        int port = port();*/
        logger.info("Starting DMCC scanner at URI= {} host = {} port= {}", uri, host(), port());
        logger.info("Starting observation for scanner URI = {}", uri);

        try {
            // بررسی اتصال اولیه
            Response r = dmcc.sendCommand("UPTIME");
            if (r instanceof Response.NoResponse) {
                logger.warn("No response from UPTIME, triggering disconnect");
                getContext().getSelf().tell(new ScannerCommand.OnDisconnect());
                getContext().getSelf().tell(new ScannerCommand.Connect());
                return this;
            }
            logger.info("UPTIME response = {}", r);
            // خواندن تنظیمات رنج
            Optional <Long> rangeMax = org.example.akka.util.ScannerUtils.getProperty(delegate,Long.class, "triggerRangeMax");
            Optional <Long> rangeMin = org.example.akka.util.ScannerUtils.getProperty(delegate,Long.class, "triggerRangeMin");
            Optional <Long> rangeOff = org.example.akka.util.ScannerUtils.getProperty(delegate,Long.class, "triggerRangeOff");

            boolean checkRange = rangeMin != null && rangeMax != null && rangeOff != null;
            if (!checkRange) {
                logger.info("Range check not configured");
                return this;
            }
            Optional<RangeObserverConfig> rangeConfigOpt =
                    rangeMin.flatMap(min ->
                            rangeMax.flatMap(max ->
                                    rangeOff.map(off -> new RangeObserverConfig(
                                            dmcc, cmId, min, max, off,
                                            getContext().getSelf(), uri, config.host, config.port, config.scanReceiver
                                    ))
                            )
                    );

            if (rangeConfigOpt.isPresent()) {
                RangeObserverConfig rangeConfig = rangeConfigOpt.get();
                rangeObserverActor = getContext().spawn(
                        RangeObserverActor.create(rangeConfig),
                        "rangeObserver-" + cmId
                );
                getContext().watch(rangeObserverActor);
                logger.info("RangeObserverActor created");

                rangeObserverActor.tell(new RangeObserverCommand.StartObserving());
                isRangeObserving = true;
                logger.info("RangeObserverActor observing started");
            } else {
                logger.warn("Range observation skipped — one or more range properties were missing.");
            }


        } catch (Throwable t) {
            logger.error("Failed to start:", t);
        }
        return this;
        // اطلاعات اتصال
    }

/*    public   <T> Optional<T> getProperty(Class<T> clazz, String propertyName) {
        Object value = delegate.getSingle(LOGISTICS.NAMAESPACE_URI.appendLocalPart(propertyName));
        if (value == null) return Optional.empty();

        try {
            if (clazz == Long.class) {
                return Optional.of(clazz.cast(Long.valueOf(value.toString())));
            } else if (clazz == Boolean.class) {
                return Optional.of(clazz.cast(Boolean.valueOf(value.toString())));
            }
        } catch (Exception e) {
            logger.warn("Failed to convert property {} to type {}", propertyName, clazz.getSimpleName(), e);
        }

        return Optional.empty();
    }*/

  /*  private <T> T getProperty(Class<T> clazz, String propertyName ) {
    Object value = ((IResource) getBehaviourDelegate()).getSingle(LOGISTICS.NAMAESPACE_URI.appendLocalPart
            (propertyName));
    if (null== value) return null;
    if (clazz.isAssignableFrom(Long.class)) return (T) Long.valueOf(value.toString());
    if (clazz.isAssignableFrom(Boolean.class)) return (T) Boolean.valueOf(value.toString());
    return null;
} */

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
