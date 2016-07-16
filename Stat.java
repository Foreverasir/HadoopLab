import com.google.common.util.concurrent.AtomicDouble;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by darnell on 7/14/16.
 */

public class Stat {
    static AtomicInteger count = new AtomicInteger(0);
    static AtomicDouble lossing = new AtomicDouble(0.0);
    static AtomicBoolean isok = new AtomicBoolean(false);

}
