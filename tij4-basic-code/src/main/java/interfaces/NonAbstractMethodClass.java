package interfaces;

abstract class Base{

    public String name;
    public Integer age;

    public void print(String msg){
        System.out.print(msg);
    }

//    abstract void println(String msg);

    public static void main(String[] args) {
        System.out.println("abstract class main method.");
    }

}


public class NonAbstractMethodClass extends Base{

    public static void main(String[] args) {

//        Base base = new Base();

        NonAbstractMethodClass namc = new NonAbstractMethodClass();

        namc.print("abc");
    }

}
