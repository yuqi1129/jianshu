package com.yuqi.jianshu.hivetest;

/**
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.DOT;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_ALLCOLREF;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_DESTINATION;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_FROM;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_GROUPBY;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_HAVING;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_INSERT;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_INSERT_INTO;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_JOIN;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_PARTSPEC;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_QUERY;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_SELECT;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_SUBQUERY;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_SUBQUERY_EXPR;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_TAB;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_TABLE_OR_COL;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_TABNAME;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_TABREF;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_UNIONALL;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_UNIONDISTINCT;
import static org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.TOK_WHERE;

 */
/**
 * Author yuqi
 * Time 2/8/19
 **/
public class HiveAstTest {

    /**
    //key is table name, value is table columns
    private Map<String, Set<String>> table = Maps.newHashMap();

    //key is alias, values is really name, full path format db.tableName
    private Map<String, String> tableAlias = Maps.newHashMap();


    private Set<String> insertTables = Sets.newHashSet();
    public Map<String, Map<String, String>> tablePartitons = Maps.newHashMap();

    private Set<String> realSelectTable = Sets.newHashSet();

     */
    //map real table to alias
    /*
        select a from table t;
        key is table
        value t
     */

    /**
    private Map<String, String> realToAlias = Maps.newHashMap();

    private Map<String, List<List<String>> > tableColumnsMap = Maps.newHashMap();
    private Stack<List<List<String>>> selectColumns = new Stack<>();


    //store current query table context
    private Stack<String> tableStack = new Stack<>();

    private AtomicInteger atomicInteger = new AtomicInteger(0);

    private static final int[] COUNTER = new int[]{0, 0};

    public static void main(String[] args) {
        //

        //问题: 1 无论识别函数名与正常的列名
        ParseDriver parseDriver = new ParseDriver();

        String sql1 = "insert into table `t3` select id.s1.s2.s3 + round(id.s1.s2) from (select id, name, age from bigdata.imeimd5_key_mapping) t";
        String sql2 = "insert into t2 select id from t1 t";


        String sql3 = "insert overwrite table bigdata.dws_bigdata_gaid_2_mid_d partition(`date` = 20190801) select a.gaid + 1, b.miid, count(*) from \n" +
                "miui_data.imei_gaid_mapping a inner join profile.imeimd5_key_mapping b on a.imei = b.imeimd5 and a.`date` = 20190801 and b.`date` = 20190801 where a.nice in (select c from t3) and a.test = 10 group by a.gaid, b.mmid having count(*) in (select id from test)";

        String sql4 = "insert into table bigdata.xx1 select a.id + d.id from t1 a join t4 d on a.id = d.id union (select b.id from t2 b  union select c.id from t3 c)";

        String sql5 = "select * from t1";

        try {
            ASTNode node = parseDriver.parse(sql3);
            HiveAstTest hiveAstTest = new HiveAstTest();
            hiveAstTest.handleQuery((ASTNode) node.getChildren().get(0));

            System.out.println(hiveAstTest.insertTables);
        } catch (Exception e) {
            e.printStackTrace();
        }
//
//        InputStreamReader reader = null;
//
//        try {
//            reader = new InputStreamReader(HiveAstTest.class.getClassLoader().getResourceAsStream("sql/error.sql"));
//            List<String> strings = IOUtils.readLines(reader);
//            //HiveConf config = new HiveConf();
//            //config.setBoolVar(HIVE_SUPPORT_SQL11_RESERVED_KEYWORDS, false);
//            //config.getAllProperties().entrySet().forEach(entry -> configuration.set((String) entry.getKey(), (String) entry.getValue()));
//
//            Configuration configuration = new Configuration();
//            configuration.set(HIVE_SUPPORT_SQL11_RESERVED_KEYWORDS.name(), "false");
//            Context context = new Context(configuration);
//            strings.forEach(s -> {
//                try {
//                    COUNTER[0]++;
//                    ASTNode node = parseDriver.parse(s, context);
//                    HiveAstTest hiveAstTest = new HiveAstTest();
//                    hiveAstTest.handleQuery((ASTNode) node.getChildren().get(0));
//                    hiveAstTest.printInfo();
//                    COUNTER[1]++;
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//
//            });
//
//
//
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        } finally {
//            if (reader != null) {
//                try {
//                    reader.close();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }

        System.out.println("ALL:" + COUNTER[0]);
        System.out.println("SUCCESS:" + COUNTER[1]);

    }


    private void printInfo() {

        System.out.println(info);

    }

    //entry point
    public void handleQuery(ASTNode astNode) {
        List<Node> childs = astNode.getChildren();
        for (Node node : childs) {
            ASTNode ch = (ASTNode) node;
            int token = ch.getToken().getType();

            switch (token) {
                case TOK_FROM:
                    handleFrom(ch);
                    break;
                case TOK_INSERT:
                    handleInsert(ch);
                    break;
                default:
                    //donoting
            }
        }
    }

    public void handleFrom(ASTNode fromNode) {
        List<Node> childs = fromNode.getChildren();

        for (Node node : childs) {
            ASTNode ch = (ASTNode) node;

            int token = ch.getToken().getType();
            switch (token) {
                case TOK_SUBQUERY:
                    handleSubQuery(ch);
                    break;
                case TOK_JOIN:
                    handleJoin(ch);
                    break;
                case TOK_TABREF:
                    handleTableRef(ch);
                    break;
                default:
                    //this should be the alias
            }
        }
    }

    public void handleInsert(ASTNode insertNode) {
        List<Node> childs = insertNode.getChildren();
        for (Node node : childs) {
            ASTNode ch = (ASTNode) node;

            int token = ch.getToken().getType();

            switch (token) {
                case TOK_INSERT_INTO:
                case TOK_DESTINATION:
                    handleDestination(ch);
                    break;
                case TOK_SELECT:
                    handleSelect(ch);
                    break;
                case TOK_WHERE:
                    handleWhere(ch);
                    break;
                case TOK_GROUPBY:
                    handleGroupBy(ch);
                    break;
                case TOK_HAVING:
                    handleHaving(ch);
                    break;
                default:
                    handleIdentifer(ch);
                    //donoting
            }
        }
    }

    public void handleSubQuery(ASTNode subQuery) {
        List<ASTNode> childNodes = subQuery.getChildren().stream()
                .map(a -> (ASTNode) a).collect(Collectors.toList());

        int ch = childNodes.get(0).getType();

        switch (ch) {
            case TOK_UNIONALL:
            case TOK_UNIONDISTINCT:
                ASTNode union = childNodes.get(0);
                union.getChildren().forEach(a -> {
                    ASTNode query = (ASTNode) a;
                    handleQuery(query);
                });
                break;
            case TOK_QUERY:
                String aliasName = handleIdentifer(childNodes.get(1));
                tableStack.push(aliasName);
                handleQuery(childNodes.get(0));

        }
    }


    public String handleIdentifer(ASTNode astNode) {
        return BaseSemanticAnalyzer.getUnescapedName(astNode);
    }

    public void handleDestination(ASTNode astNode) {
        //todo
        astNode.getChildren().forEach(node -> {
            ASTNode ch = (ASTNode) node;

            int token = ch.getToken().getType();
            switch (token) {
                case TOK_TAB:
                    ch.getChildren().forEach(b -> {
                        ASTNode no = (ASTNode) b;
                        int t = no.getType();

                        String tableName = null;
                        if (t == TOK_TABNAME) {
                            tableName = handleTableName(no);
                            insertTables.add(tableName);
                        } else if (t == TOK_PARTSPEC) {
                            tablePartitons.put(tableName, handlePartiton(no));
                        }
                    });

                    break;
                default:
                    handleIdentifer(ch);
            }
        });
    }

    public void handleJoin(ASTNode joinNode) {
        joinNode.getChildren().forEach(node -> {
            ASTNode a = (ASTNode) node;

            int t = a.getType();

            switch (t) {
                case TOK_TABREF:
                    handleTableRef(a);
                    break;
                default:
                    //this
            }
        });


    }

    public void handleTableRef(ASTNode tableRefNode) {
        List<Node> childs = tableRefNode.getChildren();
        String tableName = handleTableName((ASTNode) childs.get(0));
        String alias = childs.size() == 1 ? null : handleIdentifer((ASTNode) childs.get(1));

        realToAlias.put(tableName, alias);
    }

    public void handleSelect(ASTNode selectNode) {
        //store select list to stack
        String tableContext = null;
        if (!tableStack.isEmpty()) {
            tableContext = tableStack.pop();
        }

        if (tableContext == null) {
            tableContext = "result_0_" + atomicInteger.getAndIncrement();
        }

        List<ASTNode> nodes = selectNode.getChildren().stream()
                .map(a -> (ASTNode) a.getChildren().get(0)).collect(Collectors.toList());


        List<List<String>> selectColuns = Lists.newArrayList();
        for (ASTNode node : nodes) {
            List<String> res = Lists.newArrayList();
            handleTokOp(node, res);
            selectColuns.add(res);
        }

        tableColumnsMap.put(tableContext, selectColuns);
    }

    public void handleWhere(ASTNode astNode) {
        handleCondition(astNode);
    }

    public void handleGroupBy(ASTNode astNode) {

    }

    public void handleHaving(ASTNode astNode) {
        handleCondition(astNode);
    }

    public String handleTableName(ASTNode a) {
        return String.join(".", a.getChildren().stream()
                .map(node -> handleIdentifer((ASTNode) node)).collect(Collectors.toList()));

    }

    public Map<String, String> handlePartiton(ASTNode p) {
        Map<String, String> map = Maps.newHashMap();


        p.getChildren().forEach(a -> {
            List<Node> pair = (List<Node>) a.getChildren();
            map.put(handleIdentifer(((ASTNode) pair.get(0))), handleIdentifer(((ASTNode) pair.get(0))));
        });


        return map;
    }

    //in on, in where,in having
    public void handleTokOp(ASTNode astNode, List<String> result) {

        if (astNode == null) {
            return;
        }

        int ch = astNode.getToken().getType();
        switch (ch) {
            case DOT:
                //handle dot
                List<String> strings = Lists.newArrayList();
                handleDot(astNode, strings);
                result.add(String.join(".", strings));
                break;
            case Identifier:
                result.add(handleIdentifer(astNode));
                break;
            case TOK_ALLCOLREF:
                result.add("*");
                break;
            default:
                if (astNode.getChildren() != null) {
                    astNode.getChildren().stream().map(c -> (ASTNode) c).forEach(b -> handleTokOp(b, result));
                }
                break;
        }
    }

    public void handleDot(ASTNode dot, List<String> list) {
        List<ASTNode> node = dot.getChildren().stream()
                .map(a -> (ASTNode) a).collect(Collectors.toList());

        for (ASTNode n : node) {
            if (n.getType() == DOT) {
                handleDot(n, list);
            } else if (n.getType() == Identifier) {
                list.add(handleIdentifer(n));
            } else if (n.getType() == TOK_TABLE_OR_COL) {
                list.add(handleIdentifer((ASTNode) n.getChildren().get(0)));
            }
        }
    }

    public void handleCondition(ASTNode astNode) {
        List<Node> astNodes = astNode.getChildren();
        if (astNodes != null) {
            List<ASTNode> asts = astNodes.stream().map(a -> (ASTNode) a).collect(Collectors.toList());
            asts.forEach(a -> handleCondition(a));
        }

        //now handle self
        if (astNode.getType() == TOK_SUBQUERY_EXPR) {
           List<ASTNode> ch = astNode.getChildren().stream().map(a -> (ASTNode) a).collect(Collectors.toList());

           ch.forEach(a -> {
              if (a.getType() == TOK_QUERY) {
                  handleQuery(a);
              }
           });
        }
    }
     */
}
