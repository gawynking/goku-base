package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;


public class SubmitJob extends SqlNode {

    private String sqlString;
    private SqlParserPos pos;

    public SubmitJob(SqlParserPos pos, String sqlString){
        super(pos);
        this.pos = pos;
        this.sqlString = sqlString;
    }

    public String getSqlString(){
        System.out.println("getSqlString");
        return this.sqlString;
    }

    @Override
    public SqlNode clone(SqlParserPos sqlParserPos) {
        System.out.println("clone");
        return null;
    }

    @Override
    public void unparse(SqlWriter sqlWriter, int i, int i1) {
        sqlWriter.keyword("submit");
        sqlWriter.keyword("job");
        sqlWriter.keyword("as");
        sqlWriter.print("\n");
        sqlWriter.keyword("" + sqlString + "");
    }

    @Override
    public void validate(SqlValidator sqlValidator, SqlValidatorScope sqlValidatorScope) {
        System.out.println("validate");
    }

    @Override
    public <R> R accept(SqlVisitor<R> sqlVisitor) {
        System.out.println("accept");
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode sqlNode, Litmus litmus) {
        System.out.println("equalsDeep");
        return false;
    }
}