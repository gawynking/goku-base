//: concurrency/LiftOff.java
package concurrency; /* Added by Eclipse.py */
// Demonstration of the Runnable interface.

public class LiftOff implements Runnable {
  protected int countDown = 10; // Default
  private static int taskCount = 0;
  private final int id = taskCount++;
  public LiftOff() {}
  public LiftOff(int countDown) {
    this.countDown = countDown;
  }
  public String status() {
    return "#" + id + "(" + (countDown > 0 ? countDown : "Liftoff!") + "), ";
  }

  public int getId(){
    return id;
  }

  public void run() {
    while(countDown-- > 0) {
      System.out.print(status());
      Thread.yield();
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    System.out.println();
  }
} ///:~
