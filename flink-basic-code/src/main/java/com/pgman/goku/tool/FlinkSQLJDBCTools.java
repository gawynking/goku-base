package com.pgman.goku.tool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;


public class FlinkSQLJDBCTools {

    /**
     * 执行更新语句
     *
     * @param sql
     * @param params
     * @return
     */
    public static int executeUpdate(Connection connection, String sql, String[] params) {

        int rtn = 0;

        PreparedStatement pstmt = null;
        try {

            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setString(i + 1, params[i]);
                }
            }

            rtn = pstmt.executeUpdate();

            connection.commit();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return rtn;
    }


    /**
     * 判断记录是否存在
     *
     * @param connection
     * @param sql
     * @param params
     * @return
     */
    public static boolean isExistsOfRecord(Connection connection, String sql, String[] params) {


        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {

            pstmt = connection.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setString(i + 1, params[i]);
                }
            }

            resultSet = pstmt.executeQuery();

            if(resultSet.next()){
                return true;
            }else{
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;

    }



    /**
     * 执行select语句
     *
     * @param sql
     * @param params
     * @param callback
     */
    public static void executeQuery(Connection connection, String sql, String[] params, QueryCallback callback) {


        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            pstmt = connection.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setString(i + 1, params[i]);
                }
            }

            resultSet = pstmt.executeQuery();

            callback.process(resultSet);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 批量执行SQL语句
     *
     * @param sql
     * @param paramsList
     * @return
     */
    public static int[] executeBatch(Connection connection, String sql, List<String[]> paramsList) {

        int[] rtn = null;
        PreparedStatement pstmt = null;

        try {
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(sql);

            if (paramsList != null && paramsList.size() > 0) {
                for (String[] params : paramsList) {
                    for (int i = 0; i < params.length; i++) {
                        pstmt.setString(i + 1, params[i]);
                    }
                    pstmt.addBatch();
                }
            }

            rtn = pstmt.executeBatch();
            connection.commit();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return rtn;
    }


    /**
     * 静态内部类：查询回调接口
     */
    public static interface QueryCallback {

        void process(ResultSet rs) throws Exception;

    }

}
