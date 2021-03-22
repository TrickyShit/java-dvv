package test.cloud.lightupon;

import cloud.lightupon.Clock;
import cloud.lightupon.DVVSet;
import junit.framework.TestCase;

import java.util.*;

public class DvvSetTest extends TestCase {
    DVVSet dvvSet;

    protected void setUp() {
        dvvSet = new DVVSet();
    }

    public void testJoin() {
        Clock A = this.dvvSet.newDvv("v1");
        Clock A1 = this.dvvSet.create(A, "a");

        Clock B = dvvSet.newWithHistory(dvvSet.join(A1), "v2");
        Clock B1 = dvvSet.update(B, A1, "b");
        assertEquals(dvvSet.join(A), new ArrayList());

        List lst = new ArrayList();
        List nested = new ArrayList();
        nested.add("a");
        nested.add(1);
        lst.add(nested);
        assertEquals(dvvSet.join(A1), lst); // [["a", 1]]

        lst = new ArrayList();
        nested = new ArrayList();
        nested.add("a");
        nested.add(1);
        lst.add(nested);
        nested = new ArrayList();
        nested.add("b");
        nested.add(1);
        lst.add(nested);
        assertEquals(dvvSet.join(B1), lst); // [["a", 1], ["b", 1]]
    }

    public void testUpdate() {
        Clock A0 = this.dvvSet.create(this.dvvSet.newDvv("v1"), "a");
        List v2 = new ArrayList();
        v2.add("v2");
        Clock A1 = this.dvvSet.update(this.dvvSet.newListWithHistory(this.dvvSet.join(A0), v2), A0, "a");
        List v3 = new ArrayList();
        v3.add("v3");
        Clock A2 = this.dvvSet.update(this.dvvSet.newListWithHistory(this.dvvSet.join(A1), v3), A0, "b");
        List v4 = new ArrayList();
        v4.add("v4");
        Clock A3 = this.dvvSet.update(this.dvvSet.newListWithHistory(this.dvvSet.join(A0), v4), A0, "b");
        List v5 = new ArrayList();
        v5.add("v5");
        Clock A4 = this.dvvSet.update(this.dvvSet.newListWithHistory(this.dvvSet.join(A0), v5), A0, "a");

        List expectedEntities0 = new ArrayList();
        List nested01 = new ArrayList();
        nested01.add("a");
        nested01.add(1);
        List nested02 = new ArrayList();
        nested02.add("v1");
        nested01.add(nested02);
        expectedEntities0.add(nested01);
        Clock expectedClock0 = new Clock(expectedEntities0, new ArrayList());
        assertTrue(expectedClock0.equals(A0)); // [[["a",1,["v1"]]],[]]

        List expectedEntities1 = new ArrayList();
        List nested11 = new ArrayList();
        nested11.add("a");
        nested11.add(2);
        List nested12 = new ArrayList();
        nested12.add("v2");
        nested11.add(nested12);
        expectedEntities1.add(nested11);
        Clock expectedClock1 = new Clock(expectedEntities1, new ArrayList());
        assertTrue(expectedClock1.equals(A1)); // [[["a",2,["v2"]]],[]]
    }

    public void testSync() {
        List X = new ArrayList(); // [[["x",1,[]]],[]]
        List nested00 = new ArrayList();
        List nested01 = new ArrayList();
        nested01.add("x");
        nested01.add(1);
        nested01.add(new ArrayList());
        nested00.add(nested01);
        X.add(nested00);
        X.add(new ArrayList());

        Clock A = this.dvvSet.create(this.dvvSet.newDvv("v1"), "a");
        List v2Lst = new ArrayList();
        v2Lst.add("v2");
        Clock Y = this.dvvSet.create(this.dvvSet.newList(v2Lst), "b");
        Clock A1 = this.dvvSet.create(this.dvvSet.newListWithHistory(this.dvvSet.join(A), v2Lst), "a");
        List v3Lst = new ArrayList();
        v3Lst.add("v3");
        Clock A3 = this.dvvSet.create(this.dvvSet.newListWithHistory(this.dvvSet.join(A1), v3Lst), "b");
        Clock A4 = this.dvvSet.create(this.dvvSet.newListWithHistory(this.dvvSet.join(A1), v3Lst), "c");

        List W = new ArrayList(); // [[["a",1,[]]],[]]
        List nested10 = new ArrayList();
        List nested11 = new ArrayList();
        nested11.add("a");
        nested11.add(1);
        nested11.add(new ArrayList());
        nested10.add(nested11);
        W.add(nested11);
        W.add(new ArrayList());

        List Z = new ArrayList(); // [[["a",2,["v2","v1"]]],[]]
        List nested20 = new ArrayList();
        List nested21 = new ArrayList();
        nested21.add("a");
        nested21.add(2);
        List nested22 = new ArrayList();
        nested22.add("v2");
        nested22.add("v1");
        nested21.add(nested22);
        nested20.add(nested21);
        Z.add(nested21);
        Z.add(new ArrayList());

        Clock clock2sync0 = new Clock(W, Z);
        Clock clock2sync1 = new Clock(Z, W);
        List syncResult0 = this.dvvSet.sync(clock2sync0);
        List syncResult1 = this.dvvSet.sync(clock2sync1);
        assertEquals(syncResult0, syncResult1);

        // test list of clocks synchronization
        List clock2sync2 = new ArrayList();
        clock2sync2.add(A);
        clock2sync2.add(A1);
        List clock2sync3 = new ArrayList();
        clock2sync3.add(A1);
        clock2sync3.add(A);
        assertEquals(clock2sync2, clock2sync3);

        List clock2sync4 = new ArrayList();
        clock2sync4.add(A4);
        clock2sync4.add(A3);
        List clock2sync5 = new ArrayList();
        clock2sync5.add(A3);
        clock2sync5.add(A4);
        assertEquals(clock2sync4, clock2sync5);

        List expectedValue0 = new ArrayList(); // [[["a",2,[]], ["b",1,["v3"]], ["c",1,["v3"]]],[]]
        List nested31 = new ArrayList();
        nested31.add("a");
        nested31.add(2);
        nested31.add(new ArrayList());

        List nested32 = new ArrayList();
        nested32.add("b");
        nested32.add(1);
        nested32.add(v3Lst);

        List nested33 = new ArrayList();
        nested33.add("c");
        nested33.add(1);
        nested33.add(v3Lst);

        List nested30 = new ArrayList();
        nested30.add(nested31);
        nested30.add(nested32);
        nested30.add(nested33);
        expectedValue0.add(nested30);
        expectedValue0.add(new ArrayList());
        assertEquals(clock2sync4, expectedValue0);

        List clock2sync6 = new ArrayList();
        clock2sync6.add(X);
        clock2sync6.add(A);

        List expectedValue1 = new ArrayList(); // [[["a",1,["v1"]],["x",1,[]]],[]]
        List nested40 = new ArrayList();
        List nested41 = new ArrayList();
        nested41.add("a");
        nested41.add(1);
        List nested42 = new ArrayList();
        nested42.add("v1");
        nested41.add(nested42);

        List nested43 = new ArrayList();
        nested43.add("x");
        nested43.add(1);
        nested43.add(new ArrayList());

        nested40.add(nested41);
        nested40.add(nested43);

        expectedValue1.add(nested40);
        expectedValue1.add(new ArrayList());
        assertEquals(clock2sync6, expectedValue1);

        List clock2sync7 = new ArrayList();
        clock2sync7.add(X);
        clock2sync7.add(A);

        List clock2sync8 = new ArrayList();
        clock2sync8.add(A);
        clock2sync8.add(X);
        assertEquals(clock2sync7, clock2sync8);

        List expectedValue2 = new ArrayList(); // [[["a",1,["v1"]],["b",1,["v2"]]],[]]
        List nested50 = new ArrayList();
        List nested51 = new ArrayList();
        List nested52 = new ArrayList();
        nested52.add("a");
        nested52.add(1);
        List v1Lst = new ArrayList();
        v1Lst.add("v1");
        nested52.add(v1Lst);
        nested51.addAll(nested52);

        List nested54 = new ArrayList();
        nested54.add("b");
        nested54.add(1);
        nested54.add(v2Lst);
        nested51.add(nested54);
        nested50.add(nested52);
        nested50.add(nested54);

        expectedValue2.add(nested50);
        expectedValue2.add(new ArrayList());

        List clock2sync9 = new ArrayList();
        clock2sync9.add(A);
        clock2sync9.add(Y);
        assertEquals(clock2sync9, expectedValue2);

        List clock2sync10 = new ArrayList();
        clock2sync10.add(Y);
        clock2sync10.add(A);
        assertEquals(clock2sync10, clock2sync9);

        assertEquals(clock2sync7, clock2sync8); // the same check, to make sure original values are not modified between calls
    }

    public void testSyncUpdate() {

    }

    public void testEvent() {

    }

    public void testLess() {

    }

    public void testEqual() {
        List nested01 = new ArrayList();
        nested01.add("a");
        nested01.add(4);
        List nested04 = new ArrayList();
        nested04.add("v5");
        nested04.add("v0");
        nested01.add(nested04);

        List nested02 = new ArrayList();
        nested02.add("b");
        nested02.add(0);
        nested02.add(new ArrayList());

        List v3Lst = new ArrayList();
        v3Lst.add("v3");
        List nested03 = new ArrayList();
        nested03.add("c");
        nested03.add(1);
        nested03.add(v3Lst);

        List nested00 = new ArrayList();
        nested00.add(nested01);
        nested00.add(nested02);
        nested00.add(nested03);

        List v0Lst = new ArrayList();
        v0Lst.add("v0");
        Clock A = new Clock(nested00, v0Lst); //  [[["a",4,["v5","v0"]],["b",0,[]],["c",1,["v3"]]], ["v0"]]

        List nested05 = new ArrayList();
        nested05.add("a");
        nested05.add(4);
        List nested06 = new ArrayList();
        nested06.add("v555");
        nested06.add("v0");
        nested05.add(nested06);

        List nested07 = new ArrayList();
        nested07.add("b");
        nested07.add(0);
        nested07.add(new ArrayList());

        List nested08 = new ArrayList();
        nested08.add("c");
        nested08.add(1);
        nested08.add(v3Lst);

        List nested09 = new ArrayList();
        nested09.add(nested05);
        nested09.add(nested07);
        nested09.add(nested08);

        Clock B = new Clock(nested09, new ArrayList()); // [[["a",4,["v555","v0"]], ["b",0,[]], ["c",1,["v3"]]], []]

        List nested10 = new ArrayList();
        nested10.add("a");
        nested10.add(4);
        List nested15 = new ArrayList();
        nested15.add("v5");
        nested15.add("v0");
        nested10.add(nested15);

        List nested11 = new ArrayList();
        nested11.add("b");
        nested11.add(0);
        nested11.add(new ArrayList());

        List nested17 = new ArrayList();
        nested17.add(nested10);
        nested17.add(nested11);

        List nested13 = new ArrayList();
        nested13.add("v6");
        nested13.add("v1");

        Clock C = new Clock(nested17, nested13); // [[["a",4,["v5","v0"]],["b",0,[]]], ["v6","v1"]]

        // compare only the causal history
        assertTrue(this.dvvSet.equal(A, B));
        assertTrue(this.dvvSet.equal(B, A));
        assertTrue(this.dvvSet.equal(A, C));
        assertTrue(this.dvvSet.equal(B, C));
    }

    public void testSize() {
        List v1List = new ArrayList();
        v1List.add("v1");
        assertEquals(1, this.dvvSet.size(this.dvvSet.newList(v1List)));
    }

    public void testValues() {
        List A = new ArrayList(); //  [[["a",4,["v0","v5"]],["b",0,[]],["c",1,["v3"]]], ["v1"]]
        List nested01 = new ArrayList();
        nested01.add("a");
        nested01.add(4);
        List nested04 = new ArrayList();
        nested04.add("v0");
        nested04.add("v5");
        nested01.add(nested04);

        List nested02 = new ArrayList();
        nested02.add("b");
        nested02.add(0);
        nested02.add(new ArrayList());

        List v3Lst = new ArrayList();
        v3Lst.add("v3");
        List nested03 = new ArrayList();
        nested03.add("c");
        nested03.add(1);
        nested03.add(v3Lst);

        List nested00 = new ArrayList();
        nested00.add(nested01);
        nested00.add(nested02);
        nested00.add(nested03);
        A.add(nested00);
        List v1Lst = new ArrayList();
        v1Lst.add("v1");
        A.add(v1Lst);

        List B = new ArrayList(); //  [[["a",4,["v0","v555"]], ["b",0,[]], ["c",1,["v3"]]], []]
        List nested05 = new ArrayList();
        nested05.add("a");
        nested05.add(4);
        List nested06 = new ArrayList();
        nested06.add("v0");
        nested06.add("v555");
        nested05.add(nested06);

        List nested07 = new ArrayList();
        nested07.add("b");
        nested07.add(0);
        nested07.add(new ArrayList());

        List nested08 = new ArrayList();
        nested08.add("c");
        nested08.add(1);
        nested08.add(v3Lst);

        List nested09 = new ArrayList();
        nested09.add(nested05);
        nested09.add(nested07);
        nested09.add(nested08);
        B.add(nested09);
        B.add(new ArrayList());

        List C = new ArrayList(); //  [[["a",4,[]],["b",0,[]]], ["v1","v6"]]

        List nested10 = new ArrayList();
        nested10.add("a");
        nested10.add(4);
        nested10.add(new ArrayList());

        List nested11 = new ArrayList();
        nested11.add("b");
        nested11.add(0);
        nested11.add(new ArrayList());

        List nested12 = new ArrayList();
        nested12.add(nested10);
        nested12.add(nested11);
        C.add(nested12);
        List nested13 = new ArrayList();
        nested13.add("v1");
        nested13.add("v6");
        C.add(nested13);

        List expectedAIds = new ArrayList();
        expectedAIds.add("a");
        expectedAIds.add("b");
        expectedAIds.add("c");
        assertEquals(dvvSet.ids(A), expectedAIds);
        assertEquals(dvvSet.ids(B), expectedAIds);

        List expectedCIds = new ArrayList();
        expectedCIds.add("a");
        expectedCIds.add("b");
        assertEquals(dvvSet.ids(C), expectedCIds);

        List expectedValuesA = new ArrayList();
        expectedValuesA.add("v0");
        expectedValuesA.add("v1");
        expectedValuesA.add("v3");
        expectedValuesA.add("v5");

        List sortedValuesA = this.dvvSet.values(A);
        Collections.sort(sortedValuesA, new Comparator<String>() {
            @Override
            public int compare(String lhs, String rhs) {
                return lhs.compareTo(rhs);
            }
        });
        assertEquals(sortedValuesA, expectedValuesA);

        List expectedValuesB = new ArrayList();
        expectedValuesB.add("v0");
        expectedValuesB.add("v3");
        expectedValuesB.add("v555");

        List sortedValuesB = this.dvvSet.values(B);
        Collections.sort(sortedValuesB);
        assertEquals(sortedValuesB, expectedValuesB);

        List expectedValuesC = new ArrayList();
        expectedValuesC.add("v1");
        expectedValuesC.add("v6");

        List sortedValuesC = this.dvvSet.values(C);
        Collections.sort(sortedValuesC);
        assertEquals(sortedValuesC, expectedValuesC);
    }

}
