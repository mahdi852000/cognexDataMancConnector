package org.example.akka.extra;

import java.text.MessageFormat;

public class DmccResponse {

    public static final int STATUS_OK= 0;
    public static final int STATUS_ASYNC=1;
    public static final int STATUS_ERROR=100;
    public static final int STATUS_CMD_INVALID=101;

    public static final int STATUS_SILENT=-1;
    public static final int STATUS_NORESPONSE=-10;


    protected boolean useCheckSum;
    protected Integer id;
    protected int status;
    protected String result;
    protected Object data;

    public int status(){return status;}

    public String result(){return result;}

    public Object data(){return data;}

    public Integer id() { return id; }

    @Override
    public String toString() {
        if(null==id) {
            return MessageFormat.format("Response(status={0}) result=''{1}'')", status,result);
        }
        return MessageFormat.format("Response(status={0}) id={1} result=''{2}''", status, id, result);
    }
    public static class NoResponse extends DmccResponse {
        NoResponse(){this.status = STATUS_NORESPONSE;}

        @Override
        public String toString() {
            return "NoResponse";
        }
    }
}
