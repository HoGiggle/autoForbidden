package com.jjhu.hadoop.ruler;

import com.jjhu.hadoop.dataStructure.ReturnMsg;

/**
 * 规则类型1
 * Created by jjhu on 2014/12/18.
 */
public class TwoBasicRuler extends OneTypeRuler {
    public TwoBasicRuler(int continueLoginDays, float lowRechargeAmount, float highRechargeAmount,
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
                            result = new ReturnMsg(Integer.parseInt(items[1]), "1");
        return result;
    }
}

        /*result.add(nowDate);
        result.add(userID);
        result.add(continueloginDays);
        result.add(highRechargeAmount);
        result.add(diandianFlag);
        result.add(playedTimes);
        result.add(winTimes);
        result.add(caishenFlag);
        result.add(caishenInterval);
        result.add(caishenNewestDate);*/
