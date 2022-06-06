package com.pgman.goku.sql;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLParserFeature;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.druid.sql.visitor.VisitorFeature;
import com.pgman.goku.sql.visitor.GokuSQLOutputVisitor;

import java.util.List;

/**
 * 自定义SQL工具类
 * 依赖 druid-1.2.9
 */
public class GokuSQLUtils extends SQLUtils {

    /**
     * SQL格式化入口方法
     *
     * @param sql
     */
    public static String sqlFormat(String sql){

        String sqlStr = GokuSQLUtils.format(
                sql,
                DbType.hive,
                new FormatOption(false),
                new SQLParserFeature[]{SQLParserFeature.KeepComments}
        ).replace("\t","    ");

        return sqlStr;

    }

    /**
     * 自定义SQL格式化方法
     *
     * @param sql
     * @param dbType
     * @param option
     * @param features
     * @return
     */
    public static String format(
            String sql,
            DbType dbType,
            FormatOption option,
            SQLParserFeature[] features
    ) {
        try {
            SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType, features);
            List<SQLStatement> statementList = parser.parseStatementList();
            return toSQLString(statementList, dbType, option);
        } catch (ParserException var7) {
            return sql;
        }
    }


    /**
     * 自定义SQL格式化入口，指定自定义format类进行SQL格式化输出
     *
     * @param statementList SQL抽象语法树
     * @param dbType: 指定数据库类型
     * @param option
     * @return
     */
    public static String toSQLString(List<SQLStatement> statementList, DbType dbType, FormatOption option) {

        StringBuilder out = new StringBuilder();
        SQLASTOutputVisitor visitor = new GokuSQLOutputVisitor(out, dbType);

        visitor.setFeatures(option.features);
        boolean printStmtSeperator;
        if (DbType.sqlserver == dbType) {
            printStmtSeperator = false;
        } else {
            printStmtSeperator = DbType.oracle != dbType;
        }

        int i = 0;
        for(int size = statementList.size(); i < size; ++i) {
            SQLStatement stmt = (SQLStatement)statementList.get(i);
            if (i > 0) {
                SQLStatement preStmt = (SQLStatement)statementList.get(i - 1);
                if (printStmtSeperator && !preStmt.isAfterSemi()) {
                    visitor.print(";");
                }

                List<String> comments = preStmt.getAfterCommentsDirect();
                if (comments != null) {
                    for(int j = 0; j < comments.size(); ++j) {
                        String comment = (String)comments.get(j);
                        if (j != 0) {
                            visitor.println();
                        }

                        visitor.printComment(comment);
                    }
                }

                if (printStmtSeperator) {
                    visitor.println();
                }

                if (!(stmt instanceof SQLSetStatement)) {
                    visitor.println();
                }
            }

            stmt.accept(visitor);

            if (i == size - 1) {
                List<String> comments = stmt.getAfterCommentsDirect();
                if (comments != null) {
                    for(int j = 0; j < comments.size(); ++j) {
                        String comment = (String)comments.get(j);
                        if (j != 0) {
                            visitor.println();
                        }

                        visitor.printComment(comment);
                    }
                }
            }
        }

        return out.toString();
    }


    /**
     * 复写父类FormatOption数据结构
     */
    public static class FormatOption {
        private int features;

        public FormatOption() {
            this.features = VisitorFeature.of(new VisitorFeature[]{VisitorFeature.OutputUCase, VisitorFeature.OutputPrettyFormat});
        }

        public FormatOption(VisitorFeature... features) {
            this.features = VisitorFeature.of(new VisitorFeature[]{VisitorFeature.OutputUCase, VisitorFeature.OutputPrettyFormat});
            this.features = VisitorFeature.of(features);
        }

        public FormatOption(boolean ucase) {
            this(ucase, true);
        }

        public FormatOption(boolean ucase, boolean prettyFormat) {
            this(ucase, prettyFormat, false);
        }

        public FormatOption(boolean ucase, boolean prettyFormat, boolean parameterized) {
            this.features = VisitorFeature.of(new VisitorFeature[]{VisitorFeature.OutputUCase, VisitorFeature.OutputPrettyFormat});
            this.features = VisitorFeature.config(this.features, VisitorFeature.OutputUCase, ucase);
            this.features = VisitorFeature.config(this.features, VisitorFeature.OutputPrettyFormat, prettyFormat);
            this.features = VisitorFeature.config(this.features, VisitorFeature.OutputParameterized, parameterized);
        }

        public boolean isDesensitize() {
            return this.isEnabled(VisitorFeature.OutputDesensitize);
        }

        public void setDesensitize(boolean val) {
            this.config(VisitorFeature.OutputDesensitize, val);
        }

        public boolean isUppCase() {
            return this.isEnabled(VisitorFeature.OutputUCase);
        }

        public void setUppCase(boolean val) {
            this.config(VisitorFeature.OutputUCase, val);
        }

        public boolean isPrettyFormat() {
            return this.isEnabled(VisitorFeature.OutputPrettyFormat);
        }

        public void setPrettyFormat(boolean prettyFormat) {
            this.config(VisitorFeature.OutputPrettyFormat, prettyFormat);
        }

        public boolean isParameterized() {
            return this.isEnabled(VisitorFeature.OutputParameterized);
        }

        public void setParameterized(boolean parameterized) {
            this.config(VisitorFeature.OutputParameterized, parameterized);
        }

        public void config(VisitorFeature feature, boolean state) {
            this.features = VisitorFeature.config(this.features, feature, state);
        }

        public final boolean isEnabled(VisitorFeature feature) {
            return VisitorFeature.isEnabled(this.features, feature);
        }
    }

}
