//: concurrency/SyncObject.java
package concurrency; /* Added by Eclipse.py */
// Synchronizing on another object.

import static net.mindview.util.Print.*;

class DualSynch {
    private Object syncObject = new Object();

    public synchronized void f() { // 针对当前对象加锁
        for (int i = 0; i < 5; i++) {
            print("f()");
            Thread.yield();
        }
    }

    public void g() { // 针对syncObject对象加锁
        synchronized (syncObject) {
            for (int i = 0; i < 5; i++) {
                print("g()");
                Thread.yield();
            }
        }
    }
}

public class SyncObject {
    public static void main(String[] args) {
        final DualSynch ds = new DualSynch();
        new Thread() {
            public void run() {
                ds.f();
            }
        }.start();
        ds.g();
    }
} /* Output: (Sample)
g()
f()
g()
f()
g()
f()
g()
f()
g()
f()
*///:~
