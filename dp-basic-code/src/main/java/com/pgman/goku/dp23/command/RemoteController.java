package com.pgman.goku.dp23.command;

public class RemoteController {

    Command[] onCommands;
    Command[] offCommands;

    Command undoCommand;


    public RemoteController() {

        onCommands = new Command[5];
        offCommands = new Command[5];

        for (int i = 0; i < 5; i++) {
            onCommands[i] = new NoCommand();
            offCommands[i] = new NoCommand();
        }
    }


    public void setCommand(int no, Command onCommand, Command offCommand) {
        onCommands[no] = onCommand;
        offCommands[no] = offCommand;
    }


    // 按下on按钮
    public void onButtonWasPushed(int no) {
        onCommands[no].execute();
        undoCommand = onCommands[no];
    }

    // 按下off按钮
    public void offButtonWasPushed(int no) {
        offCommands[no].execute();
        undoCommand = offCommands[no];
    }

    // 按下撤销按钮
    public void undoButtonWasPushed() {
        undoCommand.undo();
    }

}
