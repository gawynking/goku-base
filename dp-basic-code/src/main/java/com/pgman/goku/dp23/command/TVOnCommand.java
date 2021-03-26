package com.pgman.goku.dp23.command;

public class TVOnCommand implements Command {

	TVReceiver tv;

	public TVOnCommand(TVReceiver tv) {
		super();
		this.tv = tv;
	}

	@Override
	public void execute() {
		tv.on();
	}

	@Override
	public void undo() {
		tv.off();
	}

}
