package com.jjhu.hadoop;

import com.jjhu.hadoop.command.AllRulerCommand;
import com.jjhu.hadoop.command.Command;
import com.jjhu.hadoop.dataStructure.ReturnMsg;
import com.jjhu.hadoop.ruler.OneBasicRuler;
import com.jjhu.hadoop.ruler.Ruler;
import com.jjhu.hadoop.service.AutoForbidden;

import javax.jws.soap.SOAPBinding;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**Main test, 逻辑测试
 * Created by jjhu on 2014/12/1.
 */
public class TestMain{
    public static void main(String[] args) throws SQLException {

       /* List<Ruler> rulers = new ArrayList<Ruler>();
        rulers.add(new OneBasicRuler(5,3,1,2,2));
        rulers.add(new OneBasicRuler());
        Command allCommand = new AllRulerCommand("123jjlli", rulers);

        AutoForbidden af = new AutoForbidden();
        af.setCommand(allCommand);
        ReturnMsg msg = af.whoShouldForbidden();
        System.out.println(msg.getUserId() + '\t' + msg.getType());*/

        Connection conn = null;
        Statement st = null;
        String url = "jdbc:mysql://172.16.7.201:3306/accountdb_lhj?" +
                "user=baina&password=123456&useUnicode=true&characterEncoding=UTF8";

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url);
            st = conn.createStatement();
            ResultSet rs;

            /*String sql = "select * from a_acc_data_0 where acc_id < 100";
            rs = st.executeQuery(sql);

            int i = 1;
            System.out.printf("%-4s%-8s%-12s%-12s%-10s%s", "NO","acc_id","cr_date","ls_lg_date","nick","phoneimei");
            System.out.println();
            System.out.println();
            while(rs.next()){
                int acc_id = rs.getInt("acc_id");
                int cr_date = rs.getInt("cr_date");
                int ls_lg_date = rs.getInt("ls_lg_date");
                String nick = rs.getString("nick");
                String phoneimei = rs.getString("phoneimei");

                //System.out.printf("%-4d%-8d%-12d%-12d%-10s%s", i++, acc_id, cr_date, ls_lg_date, nick, phoneimei);
                System.out.printf("%-4s%-8s%-12s%-12s%-10s%s", i++, acc_id, cr_date, ls_lg_date, nick, phoneimei);
                System.out.println();
            }*/

            String sql = "select count(*) from a_acc_data_0";
            rs = st.executeQuery(sql);
            while (rs.next()){
                System.out.println(rs.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e){
            e.printStackTrace();
        } finally {
            st.close();
            conn.close();
        }

    }
}
