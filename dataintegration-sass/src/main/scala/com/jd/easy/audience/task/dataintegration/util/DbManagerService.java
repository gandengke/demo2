package com.jd.easy.audience.task.dataintegration.util;

import com.jd.easy.audience.task.dataintegration.property.ConfigProperties;

import java.sql.*;

/**
 * jdbc工具类
 *
 * @author cdxiongmei
 * @version V1.0
 */
public class DbManagerService {
    public static final String url = "jdbc:mysql://" + ConfigProperties.JED_URL() + "/" + ConfigProperties.JED_DBNAME() + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false&user=" + ConfigProperties.JED_USER() + "&password=" + ConfigProperties.JED_PASSWORD();
    public static final String name = "com.mysql.jdbc.Driver";
    public Connection conn = null;
    public PreparedStatement pst = null;

    public static Connection getConn() {
        Connection conn = null;
        try {
            Class.forName(name); //指定连接类型
            System.out.println("JED_URL:" + ConfigProperties.JED_URL());
            conn = DriverManager.getConnection(url); //获取连接
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static PreparedStatement prepareStmt(Connection conn, String sql) {
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pstmt;
    }

    public static Statement stmt(Connection conn) {
        Statement pstmt = null;
        try {
            pstmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pstmt;
    }

    public static ResultSet executeQuery(Statement stmt, String sql) {
        System.out.println("executeQuery:" + sql);
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }

    public static int executeUpdate(Statement stmt, String sql) {
        System.out.println("executeUpdate:" + sql);
        int rs = 0;
        try {
            rs = stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }

    public static void close(Connection conn, Statement stmt,
                             PreparedStatement preStatement, ResultSet rs) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            conn = null;
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            stmt = null;
        }
        if (preStatement != null) {
            try {
                preStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            preStatement = null;
        }

        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            rs = null;
        }
    }
    public void close() {
        try {
            this.conn.close();
            this.pst.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
