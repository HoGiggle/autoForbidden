package com.jjhu.hadoop.ruler;

import com.jjhu.hadoop.dataStructure.ReturnMsg;

/**
 * OneTypeRuler类型规则的基础类，类型0
 * Created by jjhu on 2014/12/1.
 */
public class OneBasicRuler extends OneTypeRuler {
    public OneBasicRuler(int continueLoginDays, float lowRechargeAmount, float highRechargeAmount,
                         int diandianFlag, int playTimes, int caishenInterval) {
        super(continueLoginDays, lowRechargeAmount, highRechargeAmount, diandianFlag, playTimes, caishenInterval);
    }

    public ReturnMsg execute(String msg){
        ReturnMsg result = null;
        String []items = msg.split("\\|");
        if (Float.parseFloat(items[3]) <= highRechargeAmount)
            if (Integer.parseInt(items[5]) <= playTimes)
                if (Integer.parseInt(items[8]) > caishenInterval)
                    if (Integer.parseInt(items[2]) >= continueLoginDays)
                        result = new ReturnMsg(Integer.parseInt(items[1]), "0");
        return result;
    }
}
