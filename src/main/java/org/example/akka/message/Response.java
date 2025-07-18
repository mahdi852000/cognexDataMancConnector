package org.example.akka.message;

public class Response {
    String response;
    protected Integer id;
    protected boolean checkSum;
    public static final int STATUS_NORESPONSE = -10;
    protected int status;
    protected String result;

    public Response(String response, boolean checkSum, Integer id) {
        this.response = response;
        this.checkSum = checkSum;
        this.id = id;
    }
    public Response() {
        this.response = null;
        this.checkSum = false;
        this.status = 0;
    }
    public String result() {
        return result;
    }

    public static class NoResponse extends Response {
        public NoResponse(){
            this.status=STATUS_NORESPONSE;
        }

        @Override
        public String toString() {
            return  "NoResponse";
        }
    }
}
