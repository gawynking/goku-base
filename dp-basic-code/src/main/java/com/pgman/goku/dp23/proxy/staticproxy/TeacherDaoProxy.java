package com.pgman.goku.dp23.proxy.staticproxy;

// 代理类
public class TeacherDaoProxy implements ITeacherDao {

    private ITeacherDao target;

    public TeacherDaoProxy(ITeacherDao target) {
        this.target = target;
    }

    @Override
    public void teach() {
        System.out.println(" proxy before ");
        target.teach();
        System.out.println(" proxy after ");
    }

}
