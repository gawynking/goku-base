//: exceptions/StormyInning.java
package exceptions; /* Added by Eclipse.py */
// Overridden methods may throw only the exceptions
// specified in their base-class versions, or exceptions
// derived from the base-class exceptions.

class BaseballException extends Exception {}
class Foul extends BaseballException {}
class Strike extends BaseballException {}

/**
 * 基类声明异常可以强制约客户端捕获可能在覆盖后的版本中增加的异常
 */
abstract class Inning {

    public Inning() throws BaseballException {
    }

    public void event() throws BaseballException {
        // Doesn't actually have to throw anything
    }

    public abstract void atBat() throws Strike, Foul;

    public void walk() {
    } // Throws no checked exceptions

}

class StormException extends Exception {}
class RainedOut extends StormException {}
class PopFoul extends Foul {}

/**
 * event()方法在基类中存在
 */
interface Storm {
    public void event() throws RainedOut;
    public void rainHard() throws RainedOut;
}

/**
 * 继承类
 */
public class StormyInning extends Inning implements Storm {

    // OK to add new exceptions for constructors, but you must deal with the base constructor exceptions:
    // 构造器里可以添加任何异常，但需要包含基类构造器里声明的异常 ，派生类构造器不能捕获基类构造器抛出的异常
    public StormyInning() throws RainedOut, BaseballException {}
    public StormyInning(String s) throws Foul, BaseballException {}

    // Regular methods must conform to base class:
    // walk()在基类中没有声明异常，所以子类继承不可以增加声明异常  --> 通过强制派生类遵守基类方法的异常说明，对象的可替换性得到保证
//    void walk() throws PopFoul {} //Compile error


    // Interface CANNOT add exceptions to existing methods from the base class: ???????????????????????????????????????????????????
//    public void event() throws RainedOut {}

    // If the method doesn't already exist in the base class, the exception is OK:
    public void rainHard() throws RainedOut {
    }

    // You can choose to not throw any exceptions,even if the base version does:
    // 覆盖后的event()方法表名，覆写后的方法可以不抛出任何异常，即时基类定义了的异常；
    public void event() {
    }

    // Overridden methods can throw inherited exceptions:
    // atBat()抛出的异常是基类方法抛出异常Foul的子类
    public void atBat() throws PopFoul {}

    /**
     * 如果声明的是派生类，编译器只会强制你捕获这个类所抛出的异常 ，但如果向上转型，则编译器要求你排除基类声明的异常
     * @param args
     */
    public static void main(String[] args) {
        try {
            StormyInning si = new StormyInning();
            si.atBat();
        } catch (PopFoul e) {
            System.out.println("Pop foul");
        } catch (RainedOut e) {
            System.out.println("Rained out");
        } catch (BaseballException e) {
            System.out.println("Generic baseball exception");
        }
        // Strike not thrown in derived version.
        try {
            // What happens if you upcast?
            Inning i = new StormyInning();
            i.atBat();
            // You must catch the exceptions from the
            // base-class version of the method:
        } catch (Strike e) {
            System.out.println("Strike");
        } catch (Foul e) {
            System.out.println("Foul");
        } catch (RainedOut e) {
            System.out.println("Rained out");
        } catch (BaseballException e) {
            System.out.println("Generic baseball exception");
        }
    }
} ///:~
