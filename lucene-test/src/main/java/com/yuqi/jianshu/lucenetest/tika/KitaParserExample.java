package com.yuqi.jianshu.lucenetest.tika;

import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.metadata.Metadata;

import java.io.InputStream;
import java.util.Arrays;

/**
 * Author yuqi
 * Time 7/9/19
 **/
public class KitaParserExample {

    //handle doc value
    public static void main(String[] args) {

        try {
            //should be ok to do so
            InputStream inputStream = KitaParserExample.class.getClassLoader()
                    //.getResourceAsStream("file/BQL新方案分享.pdf");
                    //.getResourceAsStream("file/流计算基础.pptx");
                    .getResourceAsStream("file/凤凰网.htm");

            BodyContentHandler contentHandler = new BodyContentHandler();
            Metadata metaData = new Metadata();

            Parser pdfParser = new AutoDetectParser();
            ParseContext parseContext = new ParseContext();

            pdfParser.parse(inputStream, contentHandler, metaData, parseContext);

            Arrays.stream(metaData.names()).forEach(s -> System.out.println(s + ":" + metaData.get(s)));

            System.out.println("Content:\n" + contentHandler.toString());

        } catch (Exception e) {
            //
            e.printStackTrace();
        }
    }
}
