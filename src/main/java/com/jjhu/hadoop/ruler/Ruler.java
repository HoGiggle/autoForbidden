package com.jjhu.hadoop.ruler;

import com.jjhu.hadoop.dataStructure.ReturnMsg;

/**
 * 规则超类，被具体规则类型继承
 * Created by jjhu on 2014/12/1.
 */
public abstract class Ruler {
    public abstract ReturnMsg execute(String msg);
}
