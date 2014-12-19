package com.jjhu.hadoop.dataStructure;

/**
 * 规则运行后返回参数对象，便于更改
 * Created by jjhu on 2014/12/1.
 */
public class ReturnMsg {
    private int userId;
    private String type;

    public ReturnMsg() {
    }

    public ReturnMsg(int id, String type){
        this.userId = id;
        this.type = type;
    }
    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
