package com.pgman.goku;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.stat.TableStat.Column;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.stat.TableStat.Name;
import com.alibaba.druid.util.JdbcConstants;

public class SQLParser {

	public static void main(String[] args) {
		String sql = "insert into td(a,b,c)" + "select ta.a,tb.b,max(tc.c) C " + "from ttta ta " + "join tttb tb "
				+ "on ta.a = tb.a " + "join tttc tc on " + "tc.a = ta.a " + "where ta.a in ('1','2','3')"
				+ "group by ta.a,tb.b";
		// parseSQL(sql);

//		String excelfileName = "E:/MAS610/BASE_INDEX.xlsx";
		String excelfileName = "E:/MAS610/DETAIL_INDEX.xlsx";
//		String excelfileName = "E:/MAS610/BI_TABLE_ETL.xlsx";
//		String excelfileName = "E:/MAS610/GROSS_TABLE_ETL.xlsx";

		System.out.println(excelfileName);
		int pos = excelfileName.lastIndexOf(".");
		if (pos < 0) {
			return;
		}
		String fileExt = excelfileName.substring(pos);
		Workbook wb = null;
		Sheet sheet = null;
		Row row = null;
		Cell cell = null;
		InputStream is;
		try {
			is = new FileInputStream(excelfileName);

			if (".xls".equalsIgnoreCase(fileExt)) {
				try {
					wb = new HSSFWorkbook(is);
				} catch (IOException e1) {
					System.out.println("Open file " + excelfileName + " error!");
					return;
				}
			} else if (".xlsx".equalsIgnoreCase(fileExt)) {
				try {
					wb = new XSSFWorkbook(is);
				} catch (IOException e) {
					System.out.println("Open file " + excelfileName + " error!");
					return;
				}
			} else {
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return;
			}
		} catch (FileNotFoundException e) {
			System.out.println("File " + excelfileName + " not exists!");
			return;
		}

		int numberOfSheets = wb.getNumberOfSheets();
		// System.out.println(numberOfSheets);
		// 获取第一个sheet
		// sheet = wb.getSheetAt(0);
//		基础指标和明细指标读取Sheet1
		sheet = wb.getSheet("Sheet1");
//		BI层读取 ETL_SQL_CONF
//		sheet = wb.getSheet("ETL_SQL_CONF");
		
		// 获取最大行数
		int rownum = sheet.getPhysicalNumberOfRows();
		// System.out.println("rownum:" + rownum);

		List<String> colList = new ArrayList<String>();
		// 获取第一行
		row = sheet.getRow(0);
		// 获取最大列数
		int colnum = row.getPhysicalNumberOfCells();
		// System.out.println("colnum:" + colnum);
		for (int i = 1; i < rownum; i++) {
			row = sheet.getRow(i);
			if (row != null) {
				// for (int j=0;j<colnum;j++){
//				基础指标编号在D列
//				int j = 3;
//				明细指标编号 在B列
				int j = 1;
//				BI层表名在 B列
//				int j = 1;
				cell = row.getCell(j);
				String data = cell.getRichStringCellValue().getString();
				String tabname = data.trim().toUpperCase();
//				System.out.println("-----------------------------------------------------");
//				System.out.print(String.format("%-32s", data));
//				j = 5;
//				cell = row.getCell(j);
//				data = cell.getRichStringCellValue().getString();
//				System.out.println(data);
//				基础指标SQL在I列
				j = 9;
//				明细指标SQL在F列
				j = 5;
//				BI 层SQL在 E列
//				j = 4;
				cell = row.getCell(j);
				data = null;
				
				if (cell != null){
					data = cell.getRichStringCellValue().getString();
				}
				if(data != null && data.length() > 3){
					data = data.trim();
					data = trimChars(data,"\r");
					data = trimChars(data,"\n");
					data = trimChars(data,"\n");
					data = trimChars(data,"\r");
					data = data.trim();
					data = trimChars(data,"\"");
					
					if (data.startsWith("\"")){
						data = data.substring(1);
//						System.out.println(data);
					}
					if (data.endsWith("\"")){
						data = data.substring(0, data.length() - 1);
//						System.out.println(data);
					}
				}
				if(data == null || data.length() < 5){
					continue;
				}
				
				try {
					List<String> ret = parseSQL(data.trim(), tabname);
					colList.addAll(ret);
					colList = new ArrayList<String>(new HashSet<String>(colList));
				} catch (Exception e) {
					System.out.println("SQL语句错误 " + tabname);
					System.out.println(data);
				}

				// }
			} else {
				break;
			}
		}
		try {
			wb.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			is.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(colList.size() > 0){
			String outFile = "E:/outfile.txt";
			FileWriter fw;
			try {
				fw = new FileWriter(outFile);
			} catch (IOException e1) {
				e1.printStackTrace();
				return;
			}
			BufferedWriter bw = new BufferedWriter(fw);
			try{
				for(String e:colList){
					bw.write(e);
					bw.newLine();
					bw.flush();
					
				}
			}catch (IOException e1) {
				e1.printStackTrace();
				return;
			}finally{
				try{
				bw.close();
				fw.close();
				}
				catch(IOException e2){
					e2.printStackTrace();
				}
			}
			
		
		}
		System.out.println("over");
	}

	public static List<String> parseSQL(String sql, String tabname) {
		List<String> colList = new ArrayList<String>();
		String dbType = JdbcConstants.MYSQL;

		// 格式化输出
		String result;
		try {
			result = SQLUtils.format(sql, dbType);
//			System.out.println(result);
		} catch (Throwable e) {
			System.out.println("捕获警告");
			// System.out.println(e.toString());
			System.out.println("SQL语句错误");
			return colList;
		}
		// System.out.println(result); // 缺省大写格式
		List<SQLStatement> stmtList = SQLUtils.parseStatements(result, dbType);

		// 解析出的独立语句的个数
		// System.out.println("size is:" + stmtList.size());
		for (int i = 0; i < stmtList.size(); i++) {

			SQLStatement stmt = stmtList.get(i);
			MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
			stmt.accept(visitor);
			Map<String, String> aliasmap = visitor.getAliasMap();
			// for (Iterator iterator = aliasmap.keySet().iterator();
			// iterator.hasNext();) {
			// String key = iterator.next().toString();
			// System.out.println("[ALIAS]" + key + " - " + aliasmap.get(key));
			// }
			Set<Column> groupby_col = visitor.getGroupByColumns();
			//
			// for (Iterator iterator = groupby_col.iterator();
			// iterator.hasNext();) {
			// Column column = (Column) iterator.next();
			// System.out.println("[GROUP]" + column.toString());
			// }
			// 获取表名称
			// System.out.println("table names:");
			Map<Name, TableStat> tabmap = visitor.getTables();
			List<String> list = new ArrayList<String>();

			for (Iterator iterator = tabmap.keySet().iterator(); iterator.hasNext();) {
				Name name = (Name) iterator.next();
				String table = name.toString().toUpperCase();
				if (!table.equals(tabname)) {
					list.add(table);
				}
				// System.out.println(name.toString() + " - " +
				// tabmap.get(name).toString());
			}

			// 获取表名称
			// System.out.println("Tables : " + visitor.getTables());
			// 获取操作方法名称,依赖于表名称
			// System.out.println("Manipulation : " + visitor.getTables());
			// 获取字段名称
//			 System.out.println("fields : " + visitor.getColumns());

			Collection<Column> cols = visitor.getColumns();

			for (Column col : cols) {
				String tab = col.getTable().trim().toUpperCase();
				if (!"*".equals(tab)) {
//					if (!tab.equalsIgnoreCase(tabname)) {
						String colname = col.getFullName().trim().toUpperCase();
						if (!"*".equals(colname)) {
//							System.out.print(tabname + "\t");
//							System.out.print(tab + "\t");
//							System.out.println(colname);
							colList.add(tabname + "\t" + tab + "\t" + colname);
						}
//					}
				}
			}
		}
		return colList;
	}
	
	public static String trimChars(String srcStr, String splitter) {
	    String regex = "^" + splitter + "*|" + splitter + "*$";
	    return srcStr.replaceAll(regex, "");
	}
}
