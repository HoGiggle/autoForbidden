package com.jjhu.hadoop.ruler;

import com.jjhu.hadoop.dataStructure.ReturnMsg;

/**
 * OneTypeRuler类型规则的基础规则类
 * Created by jjhu on 2014/12/1.
 */
public class OneBasicRuler extends OneTypeRuler {
    public OneBasicRuler(int loginDay, float rechargeAmount, int rechargeByDiandian, int playTimes, int godInterval) {
        super(loginDay, rechargeAmount, rechargeByDiandian, playTimes, godInterval);
    }

    public OneBasicRuler() {
        super();
    }

    public ReturnMsg execute(String msg){
        System.out.println("OneBasicRuler:I'm coming!");
        ReturnMsg rt = new ReturnMsg();
        rt.setUserId("123456");
        rt.setType("12");
        return rt;
//        return null;
    }
}
