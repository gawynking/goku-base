//: generics/GenericsAndCovariance.java
package generics; /* Added by Eclipse.py */

import java.util.*;

public class GenericsAndCovariance {
  public static void main(String[] args) {
    // Wildcards allow covariance:
    List<? extends Fruit> flist = new ArrayList<Apple>();
    // Compile Error: can't add any type of object:
//     flist.add(new Apple());
//     flist.add(new Fruit());
//     flist.add(new Object());
    flist.add(null); // Legal but uninteresting
    // We know that it returns at least Fruit:
    Fruit f = flist.get(0);

    // 调用自建测试类
    test02();
  }


  // chavin添加
  public static void test00(){

    ArrayList<Apple> apples = new ArrayList<Apple>();

    apples.add(new Apple());
    apples.add(new Jonathan());

  }


  // chavin添加
  public static void test01(){

//    ArrayList<Fruit> apples = new ArrayList<Apple>();

  }


  public static void test02(){
    List<? extends Fruit> flist = Arrays.asList(new Apple(),new Apple());
    Apple a = (Apple)flist.get(0); // No warning
    boolean contains = flist.contains(new Apple());// Argument is 'Object'
    int i = flist.indexOf(new Apple());// Argument is 'Object'

    /**
     * 下面代码的解释
     *
     * So when you specify an ArrayList <? extends Fruit >, the argument for add( ) becomes’? extends Fruit’. From that description, the compiler cannot
     * know which specific subtype of Fruit is required there, so it won’t accept any type of Fruit.
     *
     */
//    flist.add(new Apple());

    System.out.println("a = " + a);
    System.out.println("contains = " + contains);
    System.out.println("i = " + i);
  }


} ///:~
