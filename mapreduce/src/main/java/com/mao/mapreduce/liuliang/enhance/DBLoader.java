package com.mao.mapreduce.liuliang.enhance;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @author bigdope
 * @create 2018-09-07
 **/
public class DBLoader {

    private static Connection conn = null;
    private static  Statement statement = null;
    private static ResultSet resultSet = null;

    public static Map dbLoader(Map ruleMap) {

        try {
            Class.forName("com.mysql.jdbc.Driver");
//            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://192.168.2.2:3306/url_content_analyse", "root", "123456");

            statement = conn.createStatement();
            resultSet = statement.executeQuery("SELECT url, info FROM url_rule");
            while (resultSet.next()) {
                ruleMap.put(resultSet.getString(1), resultSet.getString(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeSource();
        }
        return ruleMap;
    }

    private static void closeSource() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Map<String, String> ruleMap = new HashMap<>();
        DBLoader.dbLoader(ruleMap);
        System.out.println(ruleMap.size());
        String s = ruleMap.get("120.196.100.55");
        System.out.println(s);
    }

}
