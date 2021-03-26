package com.pgman.goku.dp23.template;

/**
 * 1) 制作豆浆的流程 选材--->添加配料--->浸泡--->放到豆浆机打碎
 * 2) 通过添加不同的配料，可以制作出不同口味的豆浆
 * 3) 选材、浸泡和放到豆浆机打碎这几个步骤对于制作每种口味的豆浆都是一样的
 * 4) 请使用 模板方法模式 完成
 */
public abstract class SoyaMilk {

    // 模板方法
    final void make() {

        select();
        if(customerWantCondiments()){
            addCondiments();
        }
        soak();
        beat();

    }

    void select() {
        System.out.println(" select ");
    }

    abstract void addCondiments();

    void soak() {
        System.out.println(" soak ");
    }

    void beat() {
        System.out.println(" beat ");
    }

    // 钩子方法
    boolean customerWantCondiments() {
        return true;
    }

}
