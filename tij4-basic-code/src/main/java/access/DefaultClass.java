package access;

class DefaultClass {

    public String name;
    public Integer age;

    public DefaultClass(String name,Integer age){
        this.name = name;
        this.age = age;
    }

    public String toString(){
        return "name : " + name + ",age : " + age;

    }

    public static void main(String[] args) {

        DefaultClass df = new DefaultClass("chavin",30);

        System.out.println(df.toString());

    }


}
