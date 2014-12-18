package com.jjhu.hadoop.dataStructure;

/**
 * Created by jjhu on 2014/12/18.
 */
public enum LogTag {
    USERACTION(1),
    PROPSCHANGE(2),
    RECHARGE(3),
    HORN(4),
    CHAT(5),
    TASK(6),
    MATCH(7),
    CAISHENSERVER(8);

    private int value;
    LogTag(int v) {
        value = v;
    }

    public int getValue(){
        return value;
    }
}
