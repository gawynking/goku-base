//: concurrency/waxomatic/WaxOMatic.java
// Basic task cooperation.
package concurrency.waxomatic;

import java.util.concurrent.*;

import static net.mindview.util.Print.*;


class Car {

    private boolean waxOn = false; // 途蜡

    public synchronized void waxed() {
        waxOn = true; // Ready to buff
        notifyAll();
    }

    public synchronized void buffed() { // 抛光
        waxOn = false; // Ready for another coat of wax
        notifyAll();
    }

    public synchronized void waitForWaxing() throws InterruptedException {
        while (waxOn == false)
            wait();
    }

    public synchronized void waitForBuffing() throws InterruptedException {
        while (waxOn == true)
            wait();
    }

}


class WaxOn implements Runnable { // 途蜡操作

    private Car car;

    public WaxOn(Car c) {
        car = c;
    }

    public void run() {
        try {
            while (!Thread.interrupted()) {
                printnb("Wax On! \n");
                TimeUnit.MILLISECONDS.sleep(200);
                car.waxed();
                car.waitForBuffing();
            }
        } catch (InterruptedException e) {
            print("Exiting via interrupt");
        }
        print("Ending Wax On task");
    }

}


class WaxOff implements Runnable { // 抛光操作

    private Car car;

    public WaxOff(Car c) {
        car = c;
    }

    public void run() {
        try {
            while (!Thread.interrupted()) {
                car.waitForWaxing();
                printnb("Wax Off! \n");
                TimeUnit.MILLISECONDS.sleep(200);
                car.buffed();
                car.waitForWaxing();
            }
        } catch (InterruptedException e) {
            print("Exiting via interrupt");
        }
        print("Ending Wax Off task");
    }

}


public class WaxOMatic {
    public static void main(String[] args) throws Exception {

        ExecutorService exec = Executors.newCachedThreadPool();

        Car car = new Car();
        exec.execute(new WaxOff(car));
        exec.execute(new WaxOn(car));

        TimeUnit.SECONDS.sleep(5); // Run for a while...
        exec.shutdownNow(); // Interrupt all tasks

    }
} /* Output: (95% match)
Wax On! Wax Off! Wax On! Wax Off! Wax On! Wax Off! Wax On! Wax Off! Wax On! Wax Off! Wax On! Wax Off! Wax On! Wax Off! Wax On! Wax Off! Wax On! Wax Off! Wax On! Wax Off! Wax On! Wax Off! Wax On! Wax Off! Wax On! Exiting via interrupt
Ending Wax On task
Exiting via interrupt
Ending Wax Off task
*///:~
