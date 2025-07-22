import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.concurrent.BlockingDeque;

public class DataManTcpMock {
    private static final int MAX_CONNECTION = 1;
    private static final int MAX_LENGTH=1024;

    private static final int DEAFAULT_PORT = 23;

    private static final Logger logger = LoggerFactory.getLogger(DataManTcpMock.class);
    private static Locale locale = Locale.US;

    private long startTS;
    private boolean stop;

 //   private BlockingDeque<String>

}
