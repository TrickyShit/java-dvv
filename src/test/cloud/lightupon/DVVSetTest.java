package test.cloud.lightupon;

import cloud.lightupon.Clock;
import cloud.lightupon.DVVSet;
import junit.framework.TestCase;

public class DVVSetTest extends TestCase {
    DVVSet dvvSet;

    protected void setUp() {
        dvvSet = new DVVSet();
    }

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

    public void UpdateTest() {
        Clock a0 = dvvSet.create(this.dvvSet.newDvv("v1"), "a");
        Clock a1 = dvvSet.update(new Clock(dvvSet.join(a0), "v2"),a0, "a");
        Clock a2 = dvvSet.update(new Clock(dvvSet.join(a1), "v3"),a1, "b");
        Clock a3 = dvvSet.update(new Clock(dvvSet.join(a0), "v4"),a1, "b");
        Clock a4 = dvvSet.update(new Clock(dvvSet.join(a0), "v5"),a1, "a");
        assertEquals(dvvSet.ClockToString(a0), "[{a,1,[v1]}],[];");
        assertEquals(dvvSet.ClockToString(a1), "[{a,2,[v2]}],[];");
        assertEquals(dvvSet.ClockToString(a2), "[{a,2,[]}],[{b,1,[v3]}],[];");
        assertEquals(dvvSet.ClockToString(a3), "[{a,2,[v2]}],[{b,1,[v4]}],[];");
        assertEquals(dvvSet.ClockToString(a4), "[{a,3,[v5][v2]}],[];"); 
    }
}
