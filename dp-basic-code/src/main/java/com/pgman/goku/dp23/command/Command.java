package com.pgman.goku.dp23.command;

/**
 * 智能家电项目:
 * 1) 我们买了一套智能家电，有照明灯、风扇、冰箱、洗衣机，我们只要在手机上安装app就可以控制对这些家电工作。
 * 2) 这些智能家电来自不同的厂家，我们不想针对每一种家电都安装一个App，分别控制，我们希望只要一个app就可以控制全部智能家电。
 * 3) 要实现一个app控制所有智能家电的需要，则每个智能家电厂家都要提供一个统一的接口给app调用
 *
 */
public interface Command {

	public void execute();
	public void undo();

}
