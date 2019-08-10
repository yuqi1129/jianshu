package com.yuqi.jianshu.antlr4;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.InputStreamReader;
import java.util.List;

/**
 * Author yuqi
 * Time 9/8/19
 **/
public class Antlr4Test {

    public static void main(String[] args) {
        int total = 0;
        int success = 0;
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(
                    Antlr4Test.class.getClassLoader().getResourceAsStream("sql/error.sql"));


            List<String> sqls = IOUtils.readLines(inputStreamReader);

            for (String sql : sqls) {
                try {
                    total++;

                    sql = sql.trim();
                    if (sql.endsWith(";")) {
                        sql = sql.substring(0, sql.length() - 1);
                    }

                    if (StringUtils.isEmpty(sql)) {
                        continue;
                    }
                    System.out.println("SQL:\n" + sql);

                    SqlBaseLexer lexer = new SqlBaseLexer(new ANTLRNoCaseStringStream(sql));
                    lexer.removeErrorListeners();
                    TokenStream tokenStream = new CommonTokenStream(lexer);

                    SqlBaseParser parser = new SqlBaseParser(tokenStream);

                    TestVisitor<String> testVisitor = new TestVisitor<String>();

                    parser.statement().accept(testVisitor);

                    System.out.println(testVisitor.getTableToTypeMap());
                    System.out.println(testVisitor.getTableToAliasMap());

                    System.out.println("--------------------------------------------------------------");
                    success++;
                } catch (Exception e) {
                    System.out.println("SQL:\n" + sql);
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        System.out.println(String.format("total = %d, success = %d", total, success));
    }



    static class ANTLRNoCaseStringStream extends ANTLRInputStream {
        public ANTLRNoCaseStringStream(String input) {
            super(input);
        }

        public int LA(int i) {
            int returnChar = super.LA(i);
            if (returnChar == -1) {
                return returnChar;
            } else {
                return returnChar == 0 ? returnChar : Character.toUpperCase((char)returnChar);
            }
        }
    }
}
