package operators;

/**
 * 按位操作符 & | ~ ^
 */
public class BitOps {

    public static void main(String[] args) {

        int x = 13,y=21,z=19;

        System.out.println("x = " + Integer.toBinaryString(x));
        System.out.println("y = " + Integer.toBinaryString(y));
        System.out.println("z = " + Integer.toBinaryString(z));

        System.out.println("x&y : " + String.valueOf(x&y) + " : " + Integer.toBinaryString(x&y));
        System.out.println("x&z : " + String.valueOf(x&z) + " : " + Integer.toBinaryString(x&z));

        System.out.println("x|y : " + String.valueOf(x&y) + " : " + Integer.toBinaryString((x|y)));
        System.out.println("x|z : " + String.valueOf(x&z) + " : " + Integer.toBinaryString(x|z));

        System.out.println("~x : " + String.valueOf(~x) + " : " + Integer.toBinaryString(~x));
        System.out.println("~y : " + String.valueOf(~x) + " : " + Integer.toBinaryString(~y));

        System.out.println("x^y : " + String.valueOf(x^y) + " : " + Integer.toBinaryString(x^y));
        System.out.println("x^z : " + String.valueOf(x^y) + " : " + Integer.toBinaryString(x^z));

    }

}
