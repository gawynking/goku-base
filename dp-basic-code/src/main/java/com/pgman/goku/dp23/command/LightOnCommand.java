package com.pgman.goku.dp23.command;

public class LightOnCommand implements Command {

	LightReceiver light;
	
	public LightOnCommand(LightReceiver light) {
		super();
		this.light = light;
	}
	
	@Override
	public void execute() {
		light.on();
	}


	@Override
	public void undo() {
		light.off();
	}

}
