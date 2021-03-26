package com.pgman.goku.dp23.command;

public class TVOffCommand implements Command {

	TVReceiver tv;

	public TVOffCommand(TVReceiver tv) {
		super();
		this.tv = tv;
	}

	@Override
	public void execute() {
		tv.off();
	}

	@Override
	public void undo() {
		tv.on();
	}

}
