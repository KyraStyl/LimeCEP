package events;

import java.util.Comparator;
import java.util.Date;

public class TimestampComparator implements Comparator<ABCEvent> {

    @Override
    public int compare(ABCEvent o1, ABCEvent o2) {

        if (o1 == null && o2 == null) {
            return 0;
        }
        if (o1 == null) {
            return -1;
        }
        if (o2 == null) {
            return 1;
        }
        return Long.compare(o1.getTimestampDate().getTime(), o2.getTimestampDate().getTime());
    }
}