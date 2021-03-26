package com.pgman.goku.dp23.facade;

/**
 * 组建一个家庭影院：
 * DVD播放器、投影仪、自动屏幕、环绕立体声、爆米花机,要求完成使用家庭影院的
 * 功能，其过程为：
 * • 直接用遥控器：统筹各设备开关
 * • 开爆米花机
 * • 放下屏幕
 * • 开投影仪
 * • 开音响
 * • 开DVD，选dvd
 * • 去拿爆米花
 * • 调暗灯光
 * • 播放
 * • 观影结束后，关闭各种设备
 * <p>
 * 解决思路：定义一个高层接口，给子系统中的一组接口提供一个一致的界面(比如在高层接口提供四个方法 ready, play, pause, end )，用来访问子系统中的一群接口
 * <p>
 * 也就是说 就是通过定义一个一致的接口(界面类)，用以屏蔽内部子系统的细节,使得调用端只需跟这个接口发生调用，而无需关心这个子系统的内部细节
 *
 * 外观模式可以理解为转换一群接口，客户只要调用一个接口，而不用调用多个接口才能达到目的。外观模式就是解决多个复杂接口带来的使用困难，起到简化用户操作的作用。
 *
 */
public class HomeTheaterFacade {

    private TheaterLight theaterLight;
    private Popcorn popcorn;
    private Stereo stereo;
    private Projector projector;
    private Screen screen;
    private DVDPlayer dVDPlayer;


    public HomeTheaterFacade() {
        super();
        this.theaterLight = TheaterLight.getInstance();
        this.popcorn = Popcorn.getInstance();
        this.stereo = Stereo.getInstance();
        this.projector = Projector.getInstance();
        this.screen = Screen.getInstance();
        this.dVDPlayer = DVDPlayer.getInstanc();
    }

    public void ready() {
        popcorn.on();
        popcorn.pop();
        screen.down();
        projector.on();
        stereo.on();
        dVDPlayer.on();
        theaterLight.dim();
    }

    public void play() {
        dVDPlayer.play();
    }

    public void pause() {
        dVDPlayer.pause();
    }

    public void end() {
        popcorn.off();
        theaterLight.bright();
        screen.up();
        projector.off();
        stereo.off();
        dVDPlayer.off();
    }

}
