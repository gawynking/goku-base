package com.pgman.goku.util;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class C3P0JDBCUtils {

	private static C3P0JDBCUtils instance = null;

	public static C3P0JDBCUtils getInstance() {
		if (instance == null) {
			synchronized (C3P0JDBCUtils.class) {
				if (instance == null) {
					instance = new C3P0JDBCUtils();
				}
			}
		}
		return instance;
	}

	private DataSource dataSource = null;

	/**
	 * 初始化连接池
	 */
	private C3P0JDBCUtils() {
	    dataSource = new ComboPooledDataSource();
	}

    /**
     * 获取连接
     *
     * @return
     */
	public synchronized Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

	/**
	 * 执行更新语句
	 *
	 * @param sql
	 * @param params
	 * @return
	 */
	public int executeUpdate(String sql, Object[] params) {
		int rtn = 0;
		Connection connection = null;
		PreparedStatement pstmt = null;

		try {
			connection = getConnection();
			connection.setAutoCommit(false);

			pstmt = connection.prepareStatement(sql);

			if (params != null && params.length > 0) {
				for (int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);
				}
			}

			rtn = pstmt.executeUpdate();

			connection.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
		}

		return rtn;
	}

	/**
	 * 执行select语句
	 *
	 * @param sql
	 * @param params
	 * @param callback
	 */
	public void executeQuery(String sql, Object[] params, QueryCallback callback) {
		Connection connection = null;
		PreparedStatement pstmt = null;
		ResultSet resultSet = null;

		try {
			connection = getConnection();
			pstmt = connection.prepareStatement(sql);

			if (params != null && params.length > 0) {
				for (int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);
				}
			}

			resultSet = pstmt.executeQuery();

			callback.process(resultSet);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
		}
	}

	/**
	 * 批量执行SQL语句
	 *
	 * @param sql
	 * @param paramsList
	 * @return
	 */
	public int[] executeBatch(String sql, List<Object[]> paramsList) {
		int[] rtn = null;
		Connection connection = null;
		PreparedStatement pstmt = null;

		try {
			connection = getConnection();
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);

			if (paramsList != null && paramsList.size() > 0) {
				for (Object[] params : paramsList) {
					for (int i = 0; i < params.length; i++) {
						pstmt.setObject(i + 1, params[i]);
					}
					pstmt.addBatch();
				}
			}

			rtn = pstmt.executeBatch();
			connection.commit();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
		}

		return rtn;
	}

	/**
	 * 静态内部类：查询回调接口
	 *
	 */
	public static interface QueryCallback {

		void process(ResultSet rs) throws Exception;

	}

}
