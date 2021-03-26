package com.pgman.goku.dp23.command;

/**
 * 没有任何命令，即空执行：用于初始化每个按钮
 * @author Administrator
 *
 */
public class NoCommand implements Command {

	@Override
	public void execute() {
	}

	@Override
	public void undo() {
	}

}
