package org.example.akka.extra;

import org.example.akka.message.Response;
import org.example.akka.message.ScannerCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;



public class TcpSystemConnector implements SystemConnector {

    protected final static Logger logger = LoggerFactory.getLogger(TcpConnector.class);

    private String host;
    private int port;

    private Socket socket;
    private OutputStream out;
    private InputStream in;

    private ScheduledExecutorService exeSvc;
    private BlockingDeque<Response> message;
    private Object writeLock;

    private Collection<SystemConnector.Listener> listeners;
    private Deque<ResponseListener> responseListeners;
    private boolean useHeartBeat;

    public TcpSystemConnector(String host) {
        this(host,23);
    }

    public TcpSystemConnector (String host, int port) {
        this.host=host;
        this.port=port;
        this.writeLock=new Object();
        this.listeners = new ArrayList<>();
        this.responseListeners = new ConcurrentLinkedDeque<>();
    }

    public TcpSystemConnector useHeartBeat(boolean on) {
        this.useHeartBeat = on;
        return this;
    }

    private class ResponseListener{
        Integer regId;
        AtomicReference<DmccResponse> responseRef= new AtomicReference<>();

        public ResponseListener(Integer regId) {this.regId = regId;}
        protected DmccResponse get() throws InterruptedException {
            synchronized (responseRef) {
                responseRef.wait(2500L);
                return responseRef.get();
            }
        }

        public boolean consume(DmccResponse response) {
            if(response.status()>= DmccResponse.STATUS_OK && null != regId && !regId.equals(response.id())) {
                logger.warn("request/response id mismatch reg-id={},ignoring response={}", regId,response);
                return false;
            }
            synchronized (responseRef) {
                responseRef.set(response);
                responseRef.notify();
            }
            responseListeners.remove(this);
            return true;
        }
    }




        @Override
        public boolean connected() {
            return false;
        }

        @Override
        public boolean disconnect() {
            return false;
        }

        @Override
        public boolean connect() {
            return false;
        }

        @Override
        public boolean addListener(Listener listener) {
           return listeners.add(listener);
        }

        @Override
        public boolean removeListener(Listener listener) {
           return listeners.remove(listener);
        }

        @Override
        public Response send(Request request) throws IOException {
            return null;
        }
}
