package com.pgman.goku.util;

import com.pgman.goku.config.ConfigurationManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

public class SimpleJDBCUtils {

	static {
		try {
			String driver = ConfigurationManager.getString("jdbc.driver");
			Class.forName(driver);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static SimpleJDBCUtils instance = null;

	public static SimpleJDBCUtils getInstance() {
		if (instance == null) {
			synchronized (SimpleJDBCUtils.class) {
				if (instance == null) {
					instance = new SimpleJDBCUtils();
				}
			}
		}
		return instance;
	}

	private LinkedList<Connection> dataSource = new LinkedList<Connection>();

	/**
	 * 初始化连接池
	 */
	private SimpleJDBCUtils() {
		int dataSourceSize = ConfigurationManager.getInteger("jdbc.datasource.size");
		for (int i = 0; i < dataSourceSize; i++) {

			String url = ConfigurationManager.getString("jdbc.url");
			String user = ConfigurationManager.getString("jdbc.user");
			String password = ConfigurationManager.getString("jdbc.password");

			try {
				Connection connection = DriverManager.getConnection(url, user, password);
				dataSource.push(connection);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public synchronized Connection getConnection() {
		while (dataSource.size() == 0) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return dataSource.poll();
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
				dataSource.push(connection);
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
				dataSource.push(connection);
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
				dataSource.push(connection);
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
