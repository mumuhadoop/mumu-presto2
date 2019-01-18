package com.lovecws.mumu.presto.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class PrestoMysqlConnector {

    public static void main(String[] args) {
        //String url = "jdbc:presto://192.168.0.25:8080/mysql/presto_test";
        String url = "jdbc:presto://192.168.0.25:8080";
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = DriverManager.getConnection(url, "root", "");
            statement = connection.createStatement();
            statement.execute("insert into mysql.presto_test.author(auth_id,auth_name,topic) values(2,'Dout','spark')");
            resultSet = statement.executeQuery("select *from mysql.presto_test.author");
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    Object object = resultSet.getObject(i);
                    System.out.print(object + " ");
                }
                System.out.println();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (resultSet != null) resultSet.close();
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
