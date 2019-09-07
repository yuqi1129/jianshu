package com.yuqi.jianshu.antlr4;


import com.google.common.collect.Maps;
import com.yuqi.jianshu.Utils;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Map;
import java.util.Stack;

/**
 * Author yuqi
 * Time 9/8/19
 **/
public class TestVisitor extends SqlBaseBaseVisitor<String> {

    private Map<String, String> tableToAliasMap = Maps.newHashMap();

    private Map<String, String> tableToTypeMap = Maps.newHashMap();

    private Stack<String> tableStack = new Stack<>();

    public Map<String, String> getTableToAliasMap() {
        return tableToAliasMap;
    }

    public Map<String, String> getTableToTypeMap() {
        return tableToTypeMap;
    }


    public TestVisitor() {
    }


    @Override public String visitTableIdentifier(SqlBaseParser.TableIdentifierContext ctx) {
        String db = ctx.db == null ? null : Utils.escapeString(ctx.db.getText());
        String table = Utils.escapeString(ctx.table.getText());

        String fullName = db == null ? table : db + "." + table;

        if (ctx.getParent() instanceof SqlBaseParser.InsertIntoContext) {
            tableToTypeMap.put(fullName, "insert");
        } else {
            tableToTypeMap.put(fullName, "select");
        }

        super.visitTableIdentifier(ctx);

        return null;
    }


    @Override
    public String visitTableName(SqlBaseParser.TableNameContext ctx) {
        ParseTree tableTree = ctx.getChild(0);
        ParseTree alias = null;
        if (ctx.getChildCount() > 1) {
            alias = ctx.getChild(1);
        }

        String tableName = Utils.escapeString(tableTree.getText());
        String aliasName = alias == null ? null : Utils.escapeString(alias.getText());

        tableToAliasMap.put(tableName, aliasName);
//        if (alias != null) {
//            tableStack.push(aliasName);
//        } else {
//            tableStack.push(tableName);
//        }

        super.visitTableName(ctx);

        return null;
    }

    @Override
    public String visitAliasedQuery(SqlBaseParser.AliasedQueryContext ctx) {
        super.visitAliasedQuery(ctx);

        SqlBaseParser.StrictIdentifierContext alias = ctx.strictIdentifier();

        //SqlBaseParser.QueryNoWithContext query = ctx.queryNoWith();

        String aliasName = alias == null ? null : Utils.escapeString(alias.getText());
        tableStack.push(aliasName);

        //should be after handle logic


        return null;
    }

//    @Override
//    public T visitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx) {
//
//        ParseTree tree = ctx.parent;
//
//       return (T) null;
//    }

//    @Override
//    public T visitNamedExpression(SqlBaseParser.NamedExpressionContext ctx) {
//
//        ParseTree tree = ctx.getChild(0);
//
//        return (T) null;super.visitAliasedQuery(ctx);
//    }


    @Override
    public String visitNamedExpressionSeq(SqlBaseParser.NamedExpressionSeqContext ctx) {
        int childCount = ctx.getChildCount();

        for (int i = 0; i < childCount; i++) {
        }

        return visitChildren(ctx);
    }

    @Override
    public String visitQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx) {
        super.visitQuerySpecification(ctx);
        return null;
    }


    //demo 中先只用表依赖，暂时不用管其它的
//    @Override
//    public T visitChildren(RuleNode node) {
//        T result = this.defaultResult();
//        int n = node.getChildCount();
//
//        //先访问From语句, 访问其它语句
//        if (node instanceof SqlBaseParser.QuerySpecificationContext) {
//            SqlBaseParser.QuerySpecificationContext context = (SqlBaseParser.QuerySpecificationContext) node;
//
//
//            int fromIndex = -1;
//            for(int i = 0; i < n && this.shouldVisitNextChild(node, result); ++i) {
//                ParseTree c = node.getChild(i);
//                if (c instanceof SqlBaseParser.FromClauseContext) {
//                    fromIndex = i;
//                    break;
//                }
//            }
//
//            if (fromIndex != -1) {
//                ParseTree c = node.getChild(fromIndex);
//                result = c.accept(this);
//            }
//
//            for(int i = 0; i < n && this.shouldVisitNextChild(node, result); ++i) {
//                if (i == fromIndex) {
//                    continue;
//                }
//                ParseTree c = node.getChild(i);
//                T childResult = c.accept(this);
//                result = this.aggregateResult(result, childResult);
//            }
//
//        } else {
//            for (int i = 0; i < n && this.shouldVisitNextChild(node, result); ++i) {
//                ParseTree c = node.getChild(i);
//                T childResult = c.accept(this);
//                result = this.aggregateResult(result, childResult);
//            }
//        }
//
//        return result;
//    }

}
