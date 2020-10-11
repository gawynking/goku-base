//: annotations/database/Member.java
package annotations.database;

@DBTable(name = "MEMBER") // 数据库表名字 member
public class Member {

    @SQLString(30)
    String firstName; // first_name varchar(30)
    @SQLString(50)
    String lastName; // last_name varchar(50)
    @SQLInteger
    Integer age; // age int
    @SQLString(value = 30, constraints = @Constraints(primaryKey = true))
    String handle; // handle varchar(30) primary key

    static int memberCount;

    public String getHandle() {
        return handle;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String toString() {
        return handle;
    }

    public Integer getAge() {
        return age;
    }

} ///:~
