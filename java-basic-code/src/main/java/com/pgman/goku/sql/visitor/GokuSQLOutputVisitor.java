package com.pgman.goku.sql.visitor;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.hive.visitor.HiveOutputVisitor;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.visitor.VisitorFeature;
import com.alibaba.druid.util.FnvHash;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;



/**
 * 定制SQL格式化工具
 */
public class GokuSQLOutputVisitor extends HiveOutputVisitor {

    {
        super.selectListNumberOfLine = 1; // 单行展示
    }


    /**
     * 复写父类构造方法
     *
     * @param appender
     */
    public GokuSQLOutputVisitor(Appendable appender) {
        super(appender);
    }

    /**
     * 复写父类构造方法
     *
     * @param appender
     * @param dbType
     */
    public GokuSQLOutputVisitor(Appendable appender, DbType dbType) {
        super(appender, dbType);
    }

    /**
     * 复写父类构造方法
     *
     * @param appender
     * @param parameterized
     */
    public GokuSQLOutputVisitor(Appendable appender, boolean parameterized) {
        super(appender, parameterized);
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*----------------------------------------------------------------------------------------------------------------*/
    /*---------------------- 子类实现方法 ------------------------------------------------------------------------------*/

    /**
     * SELECT
     *  格式化select子句
     *
     * @param selectList
     */
    protected void printSelectList(List<SQLSelectItem> selectList) {

        ++this.indentCount;
        int i = 0;
        int lineItemCount = 0;

        // 定义变量记录上一条记录是否有尾部注释
        boolean preItemHasAfterComment = false;

        for(int size = selectList.size(); i < size; ++lineItemCount) {
            SQLSelectItem selectItem = (SQLSelectItem)selectList.get(i);
            SQLExpr selectItemExpr = selectItem.getExpr();
            int paramCount = paramCount(selectItemExpr);
            boolean methodOrBinary = !(selectItemExpr instanceof SQLName) && (selectItemExpr instanceof SQLMethodInvokeExpr || selectItemExpr instanceof SQLAggregateExpr || selectItemExpr instanceof SQLBinaryOpExpr);
            if (methodOrBinary) {
                lineItemCount += paramCount - 1;
            }

            if (i != 0) {

                if(preItemHasAfterComment) {
                    preItemHasAfterComment = false;
                }else {
                    this.print0(", "); // 在字段最后加,
                }


                SQLSelectItem preSelectItem = (SQLSelectItem)selectList.get(i - 1);
                if (preSelectItem.getAfterCommentsDirect() != null) {
                    lineItemCount = 0;
                    this.println();
                } else if (methodOrBinary) {
                    if (lineItemCount >= this.selectListNumberOfLine) {
                        lineItemCount = paramCount;
                        this.println();
                    }
                } else if (lineItemCount >= this.selectListNumberOfLine || selectItemExpr instanceof SQLQueryExpr || selectItemExpr instanceof SQLCaseExpr || selectItemExpr instanceof SQLCharExpr && ((SQLCharExpr)selectItemExpr).getText().length() > 20) {
                    lineItemCount = 0;
                    this.println();
                }

//                this.print0(","); // 注释掉前,
            }else {
                println(); // 首行换行
            }

            if (selectItem.getClass() == SQLSelectItem.class) {
                this.visit(selectItem);
            } else {
                selectItem.accept(this);
            }

            if (selectItem.hasAfterComment()) {
                preItemHasAfterComment = true;
//                this.print(' ');
                this.print(", ");
                this.printlnComment(selectItem.getAfterCommentsDirect());
            }

            ++i;
        }

        --this.indentCount;
    }

    /**
     * WITH
     *  格式化with子句
     * @param x
     * @return
     */
    public boolean visit(SQLWithSubqueryClause x) {
        if (!this.isParameterized() && this.isPrettyFormat() && x.hasBeforeComment()) {
            this.printlnComments(x.getBeforeCommentsDirect());
        }

        this.print0(this.ucase ? "WITH " : "with ");
        if (x.getRecursive() == Boolean.TRUE) {
            this.print0(this.ucase ? "RECURSIVE " : "recursive ");
        }

//        ++this.indentCount;
        this.printlnAndAccept(x.getEntries(), ", ");
//        --this.indentCount;
        println();
        return false;
    }


    /**
     * SQLAggregate
     *  SQL聚合函数格式化
     *
     * @param x
     * @return
     */
    public boolean visit(SQLAggregateExpr x) {

        boolean parameterized = this.parameterized;
        if (x.methodNameHashCode64() == FnvHash.Constants.GROUP_CONCAT) {
            this.parameterized = false;
        }
        if (x.methodNameHashCode64() == FnvHash.Constants.COUNT) {
            List<SQLExpr> arguments = x.getArguments();
            if (arguments.size() == 1) {
                SQLExpr arg0 = arguments.get(0);
                if (arg0 instanceof SQLLiteralExpr) {
                    this.parameterized = false;
                }
            }
        }

        if (x.getOwner() != null) {
            printExpr(x.getOwner());
            print(".");
        }

        String methodName = x.getMethodName();
        print0(ucase ? methodName : methodName.toLowerCase());
        print('(');

        // 更改为指定大小写格式打印
        SQLAggregateOption option = x.getOption();
        if (option != null) {
            print0(this.ucase ? option.toString().toUpperCase():option.toString().toLowerCase());
            print(' ');
        }

        List<SQLExpr> arguments = x.getArguments();
        for (int i = 0, size = arguments.size(); i < size; ++i) {
            if (i != 0) {
                print0(", ");
            }
            printExpr(arguments.get(i), false);
        }

        boolean withGroup = x.isWithinGroup();
        if (withGroup) {
            print0(ucase ? ") WITHIN GROUP (" : ") within group (");
        }

        visitAggreateRest(x);

        print(')');

        if (x.isIgnoreNulls()) {
            print0(ucase ? " IGNORE NULLS" : " ignore nulls");
        }

        SQLKeep keep = x.getKeep();
        if (keep != null) {
            print(' ');
            visit(keep);
        }

        SQLOver over = x.getOver();
        if (over != null) {
            print0(ucase ? " OVER " : " over ");
            over.accept(this);
        }

        final SQLName overRef = x.getOverRef();
        if (overRef != null) {
            print0(ucase ? " OVER " : " over ");
            overRef.accept(this);
        }

        final SQLExpr filter = x.getFilter();
        if (filter != null) {
            print0(ucase ? " FILTER (WHERE " : " filter (where ");
            printExpr(filter);
            print(')');
        }

        this.parameterized = parameterized;
        return false;
    }


    /**
     * FROM
     *  格式化from子句
     *
     * @param x
     * @return
     */
    public boolean visit(SQLJoinTableSource x) {
        SQLCommentHint hint = x.getHint();
        if (hint != null) {
            hint.accept(this);
        }

        SQLTableSource left = x.getLeft();
        String alias = x.getAlias();
        if (alias != null) {
            this.print('(');
        }

        SQLJoinTableSource.JoinType joinType = x.getJoinType();
        if (left instanceof SQLJoinTableSource && ((SQLJoinTableSource)left).getJoinType() == SQLJoinTableSource.JoinType.COMMA && joinType != SQLJoinTableSource.JoinType.COMMA && DbType.postgresql != this.dbType && ((SQLJoinTableSource)left).getAttribute("ads.comma_join") == null) {
            this.print('(');
            this.printTableSource(left);
            this.print(')');
        } else {
            this.printTableSource(left);
        }

//        ++this.indentCount; // 注释缩进，目标上下表/子查询对齐
        if (joinType == SQLJoinTableSource.JoinType.COMMA) {
            this.print(',');
        } else {
            this.println();
            boolean asof = x.isAsof();
            if (asof) {
                this.print0(this.ucase ? " ASOF " : " asof");
            }

            if (x.isNatural()) {
                this.print0(this.ucase ? "NATURAL " : "natural ");
            }

            if (x.isGlobal()) {
                this.print0(this.ucase ? "GLOBAL " : "global ");
            }

            this.printJoinType(joinType);
        }

        this.print(' ');
        SQLTableSource right = x.getRight();
        if (right instanceof SQLJoinTableSource) {
            this.print('(');
            this.printTableSource(right);
            this.print(')');
        } else {
            this.printTableSource(right);
        }

        // 解析join条件
        SQLExpr condition = x.getCondition();
        if (condition != null) {
            boolean newLine = false;
            if (right instanceof SQLSubqueryTableSource) {
                newLine = true;
            } else if (condition instanceof SQLBinaryOpExpr) {
                SQLBinaryOperator op = ((SQLBinaryOpExpr)condition).getOperator();
                if (op == SQLBinaryOperator.BooleanAnd || op == SQLBinaryOperator.BooleanOr) {
                    newLine = true;
                }
            } else if (condition instanceof SQLBinaryOpExprGroup) {
                newLine = true;
            }

            ++this.indentCount;
            if (newLine) {
                this.println();
//                printIndent();
//                if(this.indentCount<=0){
//                    print('\t');
//                }
                this.print(' ');
            } else {
                this.print(' ');
            }


            this.print0(this.ucase ? "ON " : "on ");
            this.printExpr(condition, this.parameterized);
            --this.indentCount;
        }

        if (x.getUsing().size() > 0) {
            this.print0(this.ucase ? " USING (" : " using (");
            this.printAndAccept(x.getUsing(), ", ");
            this.print(')');
        }

        if (alias != null) {
            this.print0(this.ucase ? ") AS " : ") as ");
            this.print0(alias);
        }

        SQLJoinTableSource.UDJ udj = x.getUdj();
        if (udj != null) {
            this.println();
            udj.accept(this);
        }

//        --this.indentCount;
        return false;
    }


    /**
     * WHERE
     *  格式化where子句
     *
     * @param x
     * @return
     */
    public boolean visit(SQLBinaryOpExpr x) {

        SQLBinaryOperator operator = x.getOperator();
        if (this.parameterized && operator == SQLBinaryOperator.BooleanOr && !this.isEnabled(VisitorFeature.OutputParameterizedQuesUnMergeOr)) {
            x = SQLBinaryOpExpr.merge(this, x);
            operator = x.getOperator();
        }

        if (this.inputParameters != null && this.inputParameters.size() > 0 && operator == SQLBinaryOperator.Equality && x.getRight() instanceof SQLVariantRefExpr) {
            SQLVariantRefExpr right = (SQLVariantRefExpr)x.getRight();
            int index = right.getIndex();
            if (index >= 0 && index < this.inputParameters.size()) {
                Object param = this.inputParameters.get(index);
                if (param instanceof Collection) {
                    x.getLeft().accept(this);
                    this.print0(" IN (");
                    right.accept(this);
                    this.print(')');
                    return false;
                }
            }
        }

        SQLObject parent = x.getParent();
        boolean isRoot = parent instanceof SQLSelectQueryBlock;
        boolean relational = operator == SQLBinaryOperator.BooleanAnd || operator == SQLBinaryOperator.BooleanOr;
        if (isRoot && relational) {
//            ++this.indentCount;
        }

        List<SQLExpr> groupList = new ArrayList();
        SQLExpr left = x.getLeft();
        SQLExpr right = x.getRight();
        int i;
        if (this.inputParameters != null && operator != SQLBinaryOperator.Equality) {
            i = -1;
            if (right instanceof SQLVariantRefExpr) {
                i = ((SQLVariantRefExpr)right).getIndex();
            }

            Object param = null;
            if (i >= 0 && i < this.inputParameters.size()) {
                param = this.inputParameters.get(i);
            }

            if (param instanceof Collection) {
                Collection values = (Collection)param;
                if (values.size() > 0) {
                    this.print('(');
                    int valIndex = 0;
                    Iterator var13 = values.iterator();

                    while(var13.hasNext()) {
                        Object value = var13.next();
                        if (valIndex++ != 0) {
                            this.print0(this.ucase ? " OR " : " or ");
                        }

                        this.printExpr(left, this.parameterized);
                        this.print(' ');
                        if (operator == SQLBinaryOperator.Is) {
                            this.print('=');
                        } else {
                            this.printOperator(operator);
                        }

                        this.print(' ');
                        this.printParameter(value);
                    }

                    this.print(')');
                    return false;
                }
            }
        }

        if (operator.isRelational() && left instanceof SQLIntegerExpr && right instanceof SQLIntegerExpr) {
            this.print(((SQLIntegerExpr)left).getNumber().longValue());
            this.print(' ');
            this.printOperator(operator);
            this.print(' ');
            Number number = ((SQLIntegerExpr)right).getNumber();
            if (number instanceof BigInteger) {
                this.print0(((BigInteger)number).toString());
            } else {
                this.print(number.longValue());
            }

            return false;
        } else {
            while(left instanceof SQLBinaryOpExpr && ((SQLBinaryOpExpr)left).getOperator() == operator && operator != SQLBinaryOperator.IsNot && operator != SQLBinaryOperator.Is) {
                SQLBinaryOpExpr binaryLeft = (SQLBinaryOpExpr)left;
                groupList.add(binaryLeft.getRight());
                left = binaryLeft.getLeft();
            }

            groupList.add(left);

            for(i = groupList.size() - 1; i >= 0; --i) {
                SQLExpr item = (SQLExpr)groupList.get(i);
                if (relational && this.isPrettyFormat() && item.hasBeforeComment() && !this.parameterized) {
                    this.printlnComments(item.getBeforeCommentsDirect());
                }

                if (this.isPrettyFormat() && item.hasBeforeComment() && !this.parameterized) {
                    this.printlnComments(item.getBeforeCommentsDirect());
                }

                this.visitBinaryLeft(item, operator);
                if (this.isPrettyFormat() && item.hasAfterComment()) {
                    this.print(' ');
                    this.printlnComment(item.getAfterCommentsDirect());
                }

                if (i != groupList.size() - 1 && this.isPrettyFormat() && item.getParent() != null && item.getParent().hasAfterComment() && !this.parameterized) {
                    this.print(' ');
                    this.printlnComment(item.getParent().getAfterCommentsDirect());
                }

                boolean printOpSpace = true;
                if (relational) {
//                    this.println();

                    // 这块需要加判断，如果是case when 或者 方法内条件不换行
                    boolean flag = false;
                    SQLObject parentObject = x;
                    while (null != parentObject) {
                        if (parentObject instanceof SQLCaseExpr || parentObject instanceof SQLMethodInvokeExpr) {
                            flag = true;
                            break;
                        }
                        parentObject = parentObject.getParent();
                    }

                    if (flag) {
                        print(' ');
                    } else {
                        println();
                    }

                } else {
                    if (operator == SQLBinaryOperator.Modulus && DbType.oracle == this.dbType && left instanceof SQLIdentifierExpr && right instanceof SQLIdentifierExpr && ((SQLIdentifierExpr)right).getName().equalsIgnoreCase("NOTFOUND")) {
                        printOpSpace = false;
                    }

                    if (printOpSpace) {
                        this.print(' ');
                    }
                }

                this.printOperator(operator);
                if (printOpSpace) {
                    this.print(' ');
                }
            }

            this.visitorBinaryRight(x);

            if (isRoot && relational) {
//                --this.indentCount;
            }

            return false;
        }
    }


    /**
     * GROUP BY
     *  格式化group by子句
     *
     * @param x
     * @return
     */
    public boolean visit(SQLSelectGroupByClause x) {
        boolean paren = DbType.oracle == this.dbType || x.isParen();
        boolean rollup = x.isWithRollUp();
        boolean cube = x.isWithCube();
        List<SQLExpr> items = x.getItems();
        int itemSize = items.size();
        if (itemSize > 0) {
            this.print0(this.ucase ? "GROUP BY " : "group by ");
            if (x.isDistinct()) {
                this.print0(this.ucase ? "DISTINCT " : "distinct ");
            }

            if (paren && rollup) {
                this.print0(this.ucase ? "ROLLUP (" : "rollup (");
            } else if (paren && cube) {
                this.print0(this.ucase ? "CUBE (" : "cube (");
            }

            ++this.indentCount;

            for(int i = 0; i < itemSize; ++i) {
                this.println(); // 增加换行符
                SQLExpr item = (SQLExpr)items.get(i);
//                if (i != 0) {
//                    if (this.groupItemSingleLine) {
//                        this.println(", ");
//                    } else if (item instanceof SQLGroupingSetExpr) {
//                        this.println();
//                    } else {
//                        this.print(", ");
//                    }
//                }

                if (item instanceof SQLIntegerExpr) {
                    this.printInteger((SQLIntegerExpr)item, false);
                } else if (item instanceof MySqlOrderingExpr && ((MySqlOrderingExpr)item).getExpr() instanceof SQLIntegerExpr) {
                    MySqlOrderingExpr orderingExpr = (MySqlOrderingExpr)item;
                    this.printInteger((SQLIntegerExpr)orderingExpr.getExpr(), false);
                    this.print(' ' + orderingExpr.getType().name);
                } else {
                    item.accept(this);
                }

                // 打印,
                if (i != itemSize - 1) {
                    if (groupItemSingleLine) {
                        println(", ");
                    } else {
                        if (item instanceof SQLGroupingSetExpr) {
                            println();
                        } else {
                            print(", ");
                        }
                    }
                }

                SQLCommentHint hint = null;
                if (item instanceof SQLExprImpl) {
                    hint = ((SQLExprImpl)item).getHint();
                }

                if (hint != null) {
                    hint.accept(this);
                }
            }

            if (paren && (rollup || cube)) {
                this.print(')');
            }

            --this.indentCount;
        }

        if (x.getHaving() != null) {
            this.println();
            this.print0(this.ucase ? "HAVING " : "having ");
            x.getHaving().accept(this);
        }

        if (x.isWithRollUp() && !paren) {
            this.print0(this.ucase ? " WITH ROLLUP" : " with rollup");
        }

        if (x.isWithCube() && !paren) {
            this.print0(this.ucase ? " WITH CUBE" : " with cube");
        }

        return false;
    }

    /**
     * ORDER BY
     *  格式化order by子句
     *
     * @param x
     * @return
     */
    public boolean visit(SQLOrderBy x) {
        List<SQLSelectOrderByItem> items = x.getItems();
        if (items.size() > 0) {
            if (x.isSiblings()) {
                this.print0(this.ucase ? "ORDER SIBLINGS BY " : "order siblings by ");
            } else {
                this.print0(this.ucase ? "ORDER BY " : "order by ");
            }

            int i = 0;

            for(int size = items.size(); i < size; ++i) {

                SQLSelectOrderByItem item = (SQLSelectOrderByItem)items.get(i);

                if (!(item.getParent().getParent() instanceof SQLOver)) {
                    println();
                    if (this.indentCount <= 0) {
                        this.indentCount = 0;
                        print("\t");
                    }
                    printIndent();
                }

//                if (i != 0) {
//                    this.print0(", ");
//                }

//                SQLSelectOrderByItem item = (SQLSelectOrderByItem)items.get(i);
                this.visit(item);
                if (i != size - 1) { // 在排序字段后加,
                    print0(", ");
                }

            }
        }

        return false;
    }


    /**
     * ;
     *  如果输入SQL结尾有;那么输出格式化结果
     *
     * @param x
     */
    public void postVisit(SQLObject x) {
        if (x instanceof SQLStatement) {
            SQLStatement stmt = (SQLStatement)x;
            boolean printSemi = this.printStatementAfterSemi == null ? stmt.isAfterSemi() : this.printStatementAfterSemi;
            if (printSemi) {
                this.println();
                this.print(';');
                this.println();
            }
        }

    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*----------------------------------------------------------------------------------------------------------------*/
    /*---------------------- 覆写父类私有方法 ---------------------------------------------------------------------------*/
    private static int paramCount(SQLExpr x) {
        if (x instanceof SQLName) {
            return 1;
        } else if (x instanceof SQLAggregateExpr) {
            SQLAggregateExpr aggregateExpr = (SQLAggregateExpr)x;
            List<SQLExpr> args = aggregateExpr.getArguments();
            int paramCount = 1;

            SQLExpr arg;
            for(Iterator var9 = args.iterator(); var9.hasNext(); paramCount += paramCount(arg)) {
                arg = (SQLExpr)var9.next();
            }

            if (aggregateExpr.getOver() != null) {
                ++paramCount;
            }

            return paramCount;
        } else if (!(x instanceof SQLMethodInvokeExpr)) {
            if (x instanceof SQLBinaryOpExpr) {
                return paramCount(((SQLBinaryOpExpr)x).getLeft()) + paramCount(((SQLBinaryOpExpr)x).getRight());
            } else {
                return x instanceof SQLCaseExpr ? 10 : 1;
            }
        } else {
            List<SQLExpr> params = ((SQLMethodInvokeExpr)x).getArguments();
            int paramCount = 1;

            SQLExpr param;
            for(Iterator var3 = params.iterator(); var3.hasNext(); paramCount += paramCount(param)) {
                param = (SQLExpr)var3.next();
            }

            return paramCount;
        }
    }


    private void visitBinaryLeft(SQLExpr left, SQLBinaryOperator op) {
        if (left instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binaryLeft = (SQLBinaryOpExpr)left;
            SQLBinaryOperator leftOp = binaryLeft.getOperator();
            SQLObject parent = left.getParent();
            boolean leftRational = leftOp == SQLBinaryOperator.BooleanAnd || leftOp == SQLBinaryOperator.BooleanOr;
            boolean bracket;
            if (leftOp.priority > op.priority) {
                bracket = true;
            } else if (leftOp.priority == op.priority && parent instanceof SQLBinaryOpExpr && parent.getParent() instanceof SQLBinaryOpExpr && leftOp.priority == ((SQLBinaryOpExpr)parent.getParent()).getOperator().priority && left == ((SQLBinaryOpExpr)parent).getRight()) {
                bracket = true;
            } else if ((leftOp == SQLBinaryOperator.Is || leftOp == SQLBinaryOperator.IsNot) && !op.isLogical()) {
                bracket = true;
            } else if (!binaryLeft.isParenthesized()) {
                bracket = false;
            } else if (leftOp == op || (!leftOp.isLogical() || !op.isLogical()) && op != SQLBinaryOperator.Is) {
                bracket = false;
            } else {
                bracket = true;
            }

            if (bracket) {
                if (leftRational) {
                    ++this.indentCount;
                }

                this.print('(');
                this.printExpr(left, this.parameterized);
                this.print(')');
                if (leftRational) {
                    --this.indentCount;
                }
            } else {
                this.printExpr(left, this.parameterized);
            }
        } else if (left instanceof SQLBinaryOpExprGroup) {
            SQLBinaryOpExprGroup group = (SQLBinaryOpExprGroup)left;
            if (group.getOperator() == op) {
                this.visit(group);
            } else {
                ++this.indentCount;
                this.print('(');
                this.visit(group);
                this.print(')');
                --this.indentCount;
            }
        } else {
            boolean quote;
            if (left instanceof SQLInListExpr) {
                SQLInListExpr inListExpr = (SQLInListExpr)left;
                if (inListExpr.isNot()) {
                    quote = op.priority <= SQLBinaryOperator.Equality.priority;
                } else {
                    quote = op.priority < SQLBinaryOperator.Equality.priority;
                }

                if (quote) {
                    this.print('(');
                }

                this.visit(inListExpr);
                if (quote) {
                    this.print(')');
                }
            } else if (left instanceof SQLBetweenExpr) {
                SQLBetweenExpr betweenExpr = (SQLBetweenExpr)left;
                if (betweenExpr.isNot()) {
                    quote = op.priority <= SQLBinaryOperator.Equality.priority;
                } else {
                    quote = op.priority < SQLBinaryOperator.Equality.priority;
                }

                if (quote) {
                    this.print('(');
                }

                this.visit(betweenExpr);
                if (quote) {
                    this.print(')');
                }
            } else if (left instanceof SQLNotExpr) {
                this.print('(');
                this.printExpr(left);
                this.print(')');
            } else if (left instanceof SQLUnaryExpr) {
                SQLUnaryExpr unary = (SQLUnaryExpr)left;
                quote = true;
                switch(unary.getOperator()) {
                    case BINARY:
                        quote = false;
                        break;
                    case Plus:
                    case Negative:
                        quote = op.priority < SQLBinaryOperator.Add.priority;
                }

                if (quote) {
                    this.print('(');
                    this.printExpr(left);
                    this.print(')');
                } else {
                    this.printExpr(left);
                }
            } else {
                this.printExpr(left, this.parameterized);
            }
        }

    }


    private void visitorBinaryRight(SQLBinaryOpExpr x) {
        SQLExpr right = x.getRight();
        SQLBinaryOperator op = x.getOperator();
        if (this.isPrettyFormat() && right.hasBeforeComment()) {
            this.printlnComments(right.getBeforeCommentsDirect());
        }

        if (right instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binaryRight = (SQLBinaryOpExpr)right;
            SQLBinaryOperator rightOp = binaryRight.getOperator();
            boolean rightRational = rightOp == SQLBinaryOperator.BooleanAnd || rightOp == SQLBinaryOperator.BooleanOr;
            if (rightOp.priority < op.priority && (!binaryRight.isParenthesized() || rightOp == op || !rightOp.isLogical() || !op.isLogical())) {
                this.printExpr(binaryRight, this.parameterized);
            } else {
                if (rightRational) {
                    ++this.indentCount;
                }

                this.print('(');
                this.printExpr(binaryRight, this.parameterized);
                this.print(')');
                if (rightRational) {
                    --this.indentCount;
                }
            }
        } else if (right instanceof SQLBinaryOpExprGroup) {
            SQLBinaryOpExprGroup group = (SQLBinaryOpExprGroup)right;
            if (group.getOperator() == x.getOperator()) {
                this.visit(group);
            } else {
                ++this.indentCount;
                this.print('(');
                this.visit(group);
                this.print(')');
                --this.indentCount;
            }
        } else if (SQLBinaryOperator.Equality.priority < op.priority || !(right instanceof SQLInListExpr) && !(right instanceof SQLBetweenExpr) && !(right instanceof SQLNotExpr)) {
            this.printExpr(right, this.parameterized);
        } else {
            ++this.indentCount;
            this.print('(');
            this.printExpr(right, this.parameterized);
            this.print(')');
            --this.indentCount;
        }

        if (right.hasAfterComment() && this.isPrettyFormat()) {
            this.print(' ');
            this.printlnComment(right.getAfterCommentsDirect());
        }

        if (x.getHint() != null) {
            x.getHint().accept(this);
        }

    }


}
