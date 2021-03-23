package cloud.lightupon;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

public class DVVComparator implements Comparator {

    /*
     * Allows to compare lists with strings, as in Erlang.
     * ( list > string )
     */
    @Override
    public int compare(Object a, Object b) {
        if (a instanceof String && b instanceof String) {
            return (((String) a).compareTo(((String) b)));
        }
        if (a instanceof Number && b instanceof Number) {
            return (((String) a).compareTo(((String) b)));
        }
        if (a instanceof Collection && b instanceof Collection) {
            if (((Collection) a).size() > 0 && ((Collection) b).size() > 0) {
                Object a1 = ((Collection<?>) a).iterator().next();
                Object b1 = ((Collection<?>) b).iterator().next();
                if (a1 instanceof Collection && b1 instanceof Collection) {
                    int s1 = ((Collection) a1).size();
                    int s2 = ((Collection) b).size();
                    if (s1 > s2) {
                        return 1;
                    } else if (s1 == s2) {
                        return 0;
                    } else {
                        return -1;
                    }
                }
            }
        }
        if (a instanceof Collection && !(b instanceof Collection)) {
            return 1;
        }
        if (b instanceof Collection) {
            return -1;
        }
        return (((String) a).compareTo(((String) b)));
    }
}
