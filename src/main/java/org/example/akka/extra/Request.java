package org.example.akka.extra;

import lombok.Getter;

    public class Request {

        protected boolean useChecksum;
        @Getter
        protected Integer id;
        protected String cmd;
        public Request (String command) {this.cmd=command;}

        public Request id(Integer id) {
            this.id=id;
            return this;
        }


        public Request useCheckSum (boolean on) {
            this.useChecksum = on;
            return this;
        }

    }
