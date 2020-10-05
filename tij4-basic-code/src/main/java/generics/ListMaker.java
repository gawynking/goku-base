//: generics/ListMaker.java
package generics; /* Added by Eclipse.py */
import java.util.*;

public class ListMaker<T> {

  List<T> create() {
//    return new ArrayList<T>();

    return new ArrayList();
  }

  public static void main(String[] args) {
    ListMaker<String> stringMaker = new ListMaker<String>();

    ListMaker<String> stringMaker1 = new ListMaker();

    List<String> stringList = stringMaker.create();
  }

} ///:~
