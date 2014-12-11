package com.jjhu.hadoop.command;

import com.jjhu.hadoop.dataStructure.ReturnMsg;

/**
 * 命令对象定义，参照命令模式命令对象定义
 * Created by jjhu on 2014/12/1.
 */
public interface Command {
    public ReturnMsg execute();
}
