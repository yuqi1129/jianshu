package com.yuqi.jianshu.lucenetest.fullexample;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.nio.file.Paths;
import java.util.Random;

/**
 * Author yuqi
 * Time 21/7/19
 **/
public class LuceneTestOne {
    public static void main(String[] args) {

        try {
            // initialization
            Directory index = new NIOFSDirectory(Paths.get("/tmp/lucene/test_two"));
            IndexWriterConfig config = new IndexWriterConfig();
            IndexWriter writer = new IndexWriter(index, config);
            // create a document
//            Document doc = new Document();
//
//            TextField textField = new TextField("title", " Lucene - IndexWriter12 hello nice hello hello", Field.Store.YES);
//            StringField stringField = new StringField("content", " 招人，求私信 ", Field.Store.YES);
//            IntPoint intPoint = new IntPoint("age", 65536);
//            doc.add(textField);
//            doc.add(stringField);
//            doc.add(intPoint);
//
//            Document doc2 = new Document();
//
//            IntPoint point = new IntPoint("age", 2);
//            doc2.add(point);
//            writer.addDocument(doc);
//            writer.addDocument(doc2);

            Random random = new Random();
            for (int i = 0; i < 10240; i++) {
                Document d = new Document();
                IntPoint p = new IntPoint("age", i);
                d.add(p);

                writer.addDocument(d);
            }
            writer.commit();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
