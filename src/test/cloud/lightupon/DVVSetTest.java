package test.cloud.lightupon;
import java.util.List;
import org.junit.Test;
import cloud.lightupon.Clock;
import cloud.lightupon.DVVSet;
import static org.junit.Assert.assertEquals;


public class DVVSetTest{
    DVVSet dvvSet;

    protected void setUp() {
        dvvSet = new DVVSet();
    }

    @Test
    public void testErlangTests() {
        Clock A = this.dvvSet.newDvv("v1");
        Clock A1 = this.dvvSet.create(A, "a");

        Clock B = dvvSet.newWithHistory(dvvSet.join(A1), "v2");
        Clock B1 = dvvSet.update(B, A1, "b");
        assertEquals(dvvSet.ClockToString(A), "[],[v1];");
        assertEquals(dvvSet.ClockToString(A1), "[{a,1,[v1]}],[];");
        assertEquals(dvvSet.ClockToString(B), "[{a,1,[]}],[v2];");
        assertEquals(dvvSet.ClockToString(B1), "[{a,1,[]}],[{b,1,[v2]}],[];");
    }

    @Test
    public void UpdateTest() {
        dvvSet = new DVVSet();
        Clock a = this.dvvSet.newList("v1");
        Clock a0 = dvvSet.create(a, "a");
        Clock a1 = dvvSet.update(new Clock(dvvSet.join(a0), "v2"), a0, "a");
        Clock a2 = dvvSet.update(new Clock(dvvSet.join(a1), "v3"), a1, "b");
        Clock a3 = dvvSet.update(new Clock(dvvSet.join(a0), "v4"), a1, "b");
        Clock a4 = dvvSet.update(new Clock(dvvSet.join(a0), "v5"), a1, "a");
        assertEquals(dvvSet.ClockToString(a0), "[{a,1,[v1]}],[];");
        assertEquals(dvvSet.ClockToString(a1), "[{a,2,[v2]}],[];");
        assertEquals(dvvSet.ClockToString(a2), "[{a,2,[]}],[{b,1,[v3]}],[];");
        assertEquals(dvvSet.ClockToString(a3), "[{a,2,[v2]}],[{b,1,[v4]}],[];");
        assertEquals(dvvSet.ClockToString(a4), "[{a,3,[v5][v2]}],[];");
    }
    
    @Test
    public void SyncTest() {
        dvvSet = new DVVSet();
        Clock temp = this.dvvSet.newDvv("");
        List x = (List)dvvSet.create(temp, "x"); // {[{x,1,[]}],[] as Erlang
        List a = (List)dvvSet.create(this.dvvSet.newDvv("v1"), "a"); // {[{a,1,[v1]}],[]
        List y = (List)dvvSet.create(this.dvvSet.newDvv("v2"), "b");
        List a1 = (List)dvvSet.create(new Clock(dvvSet.join((Clock)a), "v2"), "a");
        List a3 = (List)dvvSet.create(new Clock(dvvSet.join((Clock)a1), "v3"), "b");
        List a4 = (List)dvvSet.create(new Clock(dvvSet.join((Clock)a1), "v3"), "c");
        // F = fun (L,R) -> L>R end;
        List w = (List)dvvSet.create(temp, "a"); //TODO W = {[{a,1,[]}],[]}
        List z = (List)dvvSet.create(temp, "a"); //TODO z = {[{a,2,[v2][v1]}],[]};
        assertEquals(dvvSet.ClockToString((Clock)dvvSet._sync(w, z)), "[{a,2,[v2]}],[];");
        assertEquals(dvvSet.ClockToString((Clock)dvvSet._sync(z, w)), "[{a,2,[v2]}],[];");
        assertEquals(dvvSet.ClockToString((Clock)dvvSet._sync(a4, a3)), "[{a,2,[]}],[{b,1,[v3]}],[{c,1,[v3]}],[];");
        assertEquals(dvvSet.ClockToString((Clock)dvvSet._sync(a3, a4)), "[{a,2,[]}],[{b,1,[v3]}],[{c,1,[v3]}],[];");
        assertEquals(dvvSet.ClockToString((Clock)dvvSet._sync(a, a1)), dvvSet.ClockToString((Clock)dvvSet._sync(a1, a)));
        assertEquals(dvvSet.ClockToString((Clock)dvvSet._sync(x, a)), "[{a,1,[v1]}],[{x,1,[]}],[];");
        assertEquals(dvvSet.ClockToString((Clock)dvvSet._sync(x, a)), dvvSet.ClockToString((Clock)dvvSet._sync(a, x)));
        assertEquals(dvvSet.ClockToString((Clock)dvvSet._sync(x, a)), dvvSet.ClockToString((Clock)dvvSet._sync(a, x)));
        assertEquals(dvvSet.ClockToString((Clock)dvvSet._sync(a, y)), "[{a,1,[v1]}],[{b,1,[v2]}],[];");
        assertEquals(dvvSet.ClockToString((Clock)dvvSet._sync(y, a)), dvvSet.ClockToString((Clock)dvvSet._sync(a, y)));
        assertEquals(dvvSet.ClockToString((Clock)dvvSet._sync(y, a)), dvvSet.ClockToString((Clock)dvvSet._sync(a, y)));
        assertEquals(dvvSet.ClockToString((Clock)dvvSet._sync(a, x)), dvvSet.ClockToString((Clock)dvvSet._sync(x, a)));
    }
}
