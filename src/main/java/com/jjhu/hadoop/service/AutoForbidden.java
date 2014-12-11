package com.jjhu.hadoop.service;

import com.jjhu.hadoop.dataStructure.ReturnMsg;
import com.jjhu.hadoop.command.Command;

/**
 * 自动封号“遥控器”，参照命令模式“遥控器”
 * Created by jjhu on 2014/12/1.
 */
public class AutoForbidden {
    Command command;

    public void setCommand(Command command) {
        this.command = command;
    }

    public ReturnMsg whoShouldForbidden(){
        return command.execute();
    }
}
