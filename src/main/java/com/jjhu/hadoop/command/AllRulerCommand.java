package com.jjhu.hadoop.command;

import com.jjhu.hadoop.dataStructure.ReturnMsg;
import com.jjhu.hadoop.ruler.Ruler;

import java.util.List;

/**
 * Created by jjhu on 2014/12/1.
 */
public class AllRulerCommand implements Command {

    String userMessage;
    List<Ruler> rulers;

    public AllRulerCommand(String userMessage, List<Ruler> rulers) {
        this.userMessage = userMessage;
        this.rulers = rulers;
    }

    @Override
    public ReturnMsg execute() {
        for (Ruler ruler : rulers){
            ReturnMsg rt = ruler.execute(userMessage);
            if (rt != null)
                return rt;
        }
        return null;
    }
}
