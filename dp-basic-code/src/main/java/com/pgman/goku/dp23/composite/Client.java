package com.pgman.goku.dp23.composite;

/**
 * 编写程序展示一个学校院系结构：需求是这样，要在一个页面中展示出学校的院系组成，一个学校有多个学院，一个学院有多个系。
 * <p>
 * 解决方案：把学校、院、系都看做是组织结构，他们之间没有继承的关系，而是一个树形结构，可以更好的实现管理操作。
 */
public class Client {

    public static void main(String[] args) {

        OrganizationComponent university = new University("qinghuadaxue", " qinghua ");

        OrganizationComponent computerCollege = new College("jisuanjixueyuan", " jisuanji ");
        OrganizationComponent infoEngineercollege = new College(" xinxigongcheng ", " xinxi ");

        university.add(computerCollege);
        university.add(infoEngineercollege);

        computerCollege.add(new Department("jisuanji", " jisuanji "));
        computerCollege.add(new Department(" ruanjiangongcheng ", " ruanjiangongcheng "));
        computerCollege.add(new Department("xixingongcheng", " xinxi "));

        infoEngineercollege.add(new Department(" xinxigongchegn ", " xinxi "));
        infoEngineercollege.add(new Department(" tongxingongcheng ", " tongxin "));

        university.print();

    }

}
