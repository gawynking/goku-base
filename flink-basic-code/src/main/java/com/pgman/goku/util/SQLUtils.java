package com.pgman.goku.util;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SQLUtils {

    private static final Logger logger = Logger.getLogger(SQLUtils.class);


    private static final List<String> sqlKeys = Arrays.asList("select", "from", "left join", "left outer join", "join", "where", "group by", "order by");


    /**
     * 从SQL文件读取sql脚本内容
     *
     * @param buffer
     * @param sqlFile
     */
    public static void readSQLScriptToBuffer(StringBuffer buffer, String sqlFile) {

        try {

            String line;

            InputStream is = new FileInputStream(sqlFile);
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));

            line = reader.readLine();
            while (line != null) {

                if (!line.trim().startsWith("--") && !"".equals(line.trim())) {
                    buffer.append(line.split("--")[0].trim());
                    buffer.append("\n");
                }
                line = reader.readLine();

            }

            reader.close();
            is.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * 读取SQL脚本有效内容
     *
     * @param sqlFile
     * @return
     */
    public static String readSQLFile(String sqlFile) {

        StringBuffer sb = new StringBuffer();
        readSQLScriptToBuffer(sb, sqlFile);
        return sb.toString();

    }


    /**
     * SQL脚本解析
     *
     * @param sqlFile
     */
    public static List[] parseSQLScript(String sqlFile) {

        // 声明封装SQL容器
        List<String> params = new ArrayList<>();
        List<String> functions = new ArrayList<>();
        List<String> tables = new ArrayList<>();
        List<String> views = new ArrayList<>();
        List<String> etlSQL = new ArrayList<>();

        // 获取sql脚本有效内容
        String sqlContent = readSQLFile(sqlFile);

        String[] sqls = sqlContent.split(";");
        for (String sql : sqls) {

            String sqlStatement = sql.trim().replaceAll("\\\\t", "\\\t");

            if (!"".equals(sqlStatement)) {

                if (sqlStatement.replaceAll("\\s", " ").toLowerCase().startsWith("set")) {
                    params.add(sqlStatement);
                } else if (sqlStatement.replaceAll("\\s", " ").toLowerCase().startsWith("create function")) {
                    functions.add(sqlStatement);
                } else if (sqlStatement.replaceAll("\\s", " ").toLowerCase().startsWith("create table")) {
                    tables.add(sqlStatement);
                } else if (sqlStatement.replaceAll("\\s", " ").toLowerCase().startsWith("create view")) {
                    views.add(sqlStatement);
                } else if (sqlStatement.replaceAll("\\s", " ").toLowerCase().startsWith("insert")) {
                    etlSQL.add(sqlStatement);
                } else if (sqlStatement.replaceAll("\\s", " ").toLowerCase().startsWith("select")) {
                    etlSQL.add(sqlStatement);
                } else {
                    logger.error("无效sql语句 ： " + sqlStatement);
                }

            }

        }

        return new List[]{params, functions, tables, views, etlSQL};

    }


    /**
     * 解析 udf SQL语句
     *
     * @param sql
     */
    public static String[] parseFunctionSQL(String sql) {

        String functionName = null;
        String functionClass = null;
        try {
            String[] elements = sql.replaceAll("\\s", " ").split(" ");
            functionName = elements[2].toLowerCase();
            functionClass = elements[4];
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new String[]{functionName, functionClass};

    }


    /**
     * 解析时间语义
     *
     * @return 1 proctime 2 rowtime
     */
    public static int getTimeCharacteristic(List<String> params) {

        for (String sql : params) {

            String[] split = sql.split("=");
            if (split.length == 2) {
                String[] split1 = split[0].trim().split("\\s");
                if (split1.length == 2) {
                    if ("flink.time.characteristic".equals(split1[1].trim().toLowerCase())) {
                        if ("rowtime".equals(split[1].trim())) {
                            return 2;
                        }
                    }
                }
            }

        }
        return 1;

    }


    /**
     * 获取Flink job级别url地址
     *
     * @param params
     * @return
     */
    public static String getCheckpointURL(List<String> params) {

        for (String sql : params) {

            String[] split = sql.split("=");
            if (split.length == 2) {
                String[] split1 = split[0].trim().split("\\s");
                if (split1.length == 2) {
                    if ("flink.job.checkpoint.path.url".equals(split1[1].trim().toLowerCase())) {
                        return split[1].trim();
                    }
                }
            }

        }
        return null;

    }



    /**
     * 检验SQL是否是upsert更新模式
     *
     * @param sql
     */
    public static boolean isUpsertMode(String sql) {

        String[] split = sql.replaceAll("\\s", " ").toLowerCase().split(" ");
        if (split.length >= 10 && "on".equals(split[3]) && "mode".equals(split[4]) && split[5].contains("upsert")) {
            return true;
        }
        return false;
    }


    /**
     * upsert sql解析
     *
     * @param sql
     * @return
     */
    public static String[] parseUpsertSQL(String sql) {

        String select = "";

        String[] split = sql.split("\\s");

        for (int i = 6; i < split.length; i++) {

            if (i == 6 && !split[i].toLowerCase().equals("select")) {
                continue;
            }

            select = select + split[i] + " ";

            if (sqlKeys.contains(split[i].toLowerCase())) {
                select = select + "\n";
            }
        }

        return new String[]{split[2].toLowerCase(), select};

    }


    /**
     * 解析 ETL SQL主键,用于更新外部数据库
     *
     * @return
     */
    public static List<String> getPrimaryKeys(String sql) {

        List<String> pkeys = new ArrayList<>();

        try {

            String primaryKeys = null;

            String pattern = "upsert[ ]*\\((.*?)\\)";
            Pattern compile = Pattern.compile(pattern);
            Matcher matcher = compile.matcher(sql.toLowerCase());
            if (matcher.find()) {
                primaryKeys = matcher.group(1);
            }

            String[] split = primaryKeys.trim().toLowerCase().split(",");
            for (int i = 0; i < split.length; i++) {
                if (StringUtils.isNotEmpty(split[i])) {
                    pkeys.add(split[i]);
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return pkeys;

    }


    /**
     * 解析sink表外部存储连接信息 - JDBC
     *
     * @return
     */
    public static Map<String, String> getJDBCInfo(String sql) {

        Map<String, String> jdbc = new HashMap<>();
        try {

            String type = null;
            String driver = null;
            String url = null;
            String username = null;
            String password = null;


            String pattern = "'connector.type'[ ]*=[ ]*'(.*?)'";
            Pattern compile = Pattern.compile(pattern);
            Matcher matcher = compile.matcher(sql.toLowerCase());
            if (matcher.find()) {
                type = matcher.group(1).trim();
            }

            if ("jdbc".equals(type.trim())) {

                // 提取jdbc driver
                pattern = "'connector.driver'[ ]*=[ ]*'(.*)'";
                compile = Pattern.compile(pattern);
                matcher = compile.matcher(sql.trim());
                if (matcher.find()) {
                    driver = matcher.group(1).trim();
                    jdbc.put("driver", driver);
                }

                // 提取jdbc url
                pattern = "'connector.url'[ ]*=[ ]*'(.*)'";
                compile = Pattern.compile(pattern);
                matcher = compile.matcher(sql.trim());
                if (matcher.find()) {
                    url = matcher.group(1).trim();
                    jdbc.put("url", url);
                }


                // 提取jdbc username
                pattern = "'connector.username'[ ]*=[ ]*'(.*)'";
                compile = Pattern.compile(pattern);
                matcher = compile.matcher(sql.trim());
                if (matcher.find()) {
                    username = matcher.group(1).trim();
                    jdbc.put("user", username);
                }


                // 提取jdbc username
                pattern = "'connector.password'[ ]*=[ ]*'(.*)'";
                compile = Pattern.compile(pattern);
                matcher = compile.matcher(sql.trim());
                if (matcher.find()) {
                    password = matcher.group(1).trim();
                    jdbc.put("password", password);
                }

            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        return jdbc;

    }


    /**
     * 通过tablename 获取table建表语句原始SQL
     *
     * @param tableName
     * @param sqls
     * @return
     */
    public static String getSinkCreateStatementFromTableName(String tableName, List<String> sqls) {

        String returnSQL = null;

        try {
            for (String sql : sqls) {

                String pattern = "create table (.*?)[ ]*\\(";
                Pattern compile = Pattern.compile(pattern);
                Matcher matcher = compile.matcher(sql.toLowerCase().trim().replaceAll("\\s", " "));
                if (matcher.find()) {
                    String name = matcher.group(1).trim();
                    if (tableName.toLowerCase().trim().equals(name)) {
                        returnSQL = sql;
                    }
                }

            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return returnSQL;

    }


    /**
     *
     * 视图语句解析
     *
     * 仅支持 create view tbl_xxx as select 方式，不支持视图定义列方式(流中使用比较鸡肋)
     *
     * @param sql
     * @return viewName viewSQL
     *
     */
    public static String[] parseViewSQL(String sql){

        String viewName = null;
        String viewSQL = "";
        try {

            String[] split = sql.trim().replaceAll("\\s", " ").split(" ");
            viewName = split[2];
            for (int i = 4; i < split.length; i++) {

                viewSQL = viewSQL + split[i] + " ";

                if (sqlKeys.contains(split[i].toLowerCase())) {
                    viewSQL = viewSQL + "\n";
                }
            }


        } catch (Exception e){
            e.printStackTrace();
        }

        return new String[]{viewName,viewSQL};

    }


}
