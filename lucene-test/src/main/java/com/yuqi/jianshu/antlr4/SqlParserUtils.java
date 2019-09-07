package com.yuqi.jianshu.antlr4;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Author yuqi
 * Time 12/8/19
 **/
public class SqlParserUtils {

    public static final Logger LOGGER = LoggerFactory.getLogger(SqlParserUtils.class);

    public static TestVisitor parseSql(String sql) {
        try {
            SqlBaseLexer lexer = new SqlBaseLexer(new Antlr4Test.ANTLRNoCaseStringStream(sql));
            lexer.removeErrorListeners();
            TokenStream tokenStream = new CommonTokenStream(lexer);

            SqlBaseParser parser = new SqlBaseParser(tokenStream);
            TestVisitor visitor = new TestVisitor();
            parser.statement().accept(visitor);

            return visitor;
        } catch (Exception e) {
            LOGGER.error(e.toString());
            throw new RuntimeException(e);
        }

    }
}
