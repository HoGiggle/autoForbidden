package com.jjhu.hadoop.ruler;

import com.jjhu.hadoop.dataStructure.ReturnMsg;

/**
 * 规则类型5
 * Created by jjhu on 2014/12/18.
 */
public class SixBasicRuler extends OneTypeRuler {
    private int winTimes;
    public SixBasicRuler(int continueLoginDays, float lowRechargeAmount, float highRechargeAmount,
                         int diandianFlag, int winTimes, int caishenInterval) {
        super(continueLoginDays, lowRechargeAmount, highRechargeAmount, diandianFlag, winTimes, caishenInterval);
        this.winTimes = winTimes;
    }
    @Override
    public ReturnMsg execute(String msg) {
        ReturnMsg result = null;
        String []items = msg.split("\\|");
        if (Float.parseFloat(items[3]) <= highRechargeAmount && Float.parseFloat(items[3]) > lowRechargeAmount)
            if (Integer.parseInt(items[6]) <= winTimes)
                if (Integer.parseInt(items[8]) > caishenInterval)
                    if (Integer.parseInt(items[2]) >= continueLoginDays)
                        if (Integer.parseInt(items[4]) == diandianFlag)
                            result = new ReturnMsg(Integer.parseInt(items[1]), "5");
        return result;
    }
}
 /*     result.add(nowDate);
        result.add(userID);
        result.add(continueloginDays);
        result.add(rechargeAmount);
        result.add(diandianFlag);
        result.add(playedTimes);
        result.add(winTimes);
        result.add(caishenFlag);
        result.add(caishenInterval);
        result.add(caishenNewestDate);*/

//"2014-12-21 00:00:00|1|6|50|0|9|2|1|9|2014-12-18 19:24:30"