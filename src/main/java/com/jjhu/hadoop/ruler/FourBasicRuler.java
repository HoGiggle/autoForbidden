package com.jjhu.hadoop.ruler;

import com.jjhu.hadoop.dataStructure.ReturnMsg;

/**
 * 规则类型3
 * Created by jjhu on 2014/12/18.
 */
public class FourBasicRuler extends OneTypeRuler {
    public FourBasicRuler(int continueLoginDays, float lowRechargeAmount, float highRechargeAmount,
                          int diandianFlag, int playTimes, int caishenInterval) {
        super(continueLoginDays, lowRechargeAmount, highRechargeAmount, diandianFlag, playTimes, caishenInterval);
    }
    @Override
    public ReturnMsg execute(String msg) {
        ReturnMsg result = null;
        String []items = msg.split("|");
        if (Float.parseFloat(items[3]) <= highRechargeAmount && Float.parseFloat(items[3]) > lowRechargeAmount)
            if (Integer.parseInt(items[5]) <= playTimes)
                if (Integer.parseInt(items[8]) > caishenInterval)
                    if (Integer.parseInt(items[2]) >= continueLoginDays)
                        if (Integer.parseInt(items[4]) == diandianFlag)
                            result = new ReturnMsg(Integer.parseInt(items[1]), "3");
        return result;
    }
}
