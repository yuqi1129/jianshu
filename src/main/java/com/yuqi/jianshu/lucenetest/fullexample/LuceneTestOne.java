package com.yuqi.jianshu.lucenetest.fullexample;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.nio.file.Paths;

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
            Document doc = new Document();

            TextField textField = new TextField("title", " Lucene - IndexWriter12 hello nice hello hello", Field.Store.YES);
            StringField stringField = new StringField("content", " 招人，求私信 ", Field.Store.YES);

            doc.add(textField);
            doc.add(stringField);


//            Document doc2 = new Document();
//
//            TextField textField2 = new TextField("title", "Lucene1 - IndexWriter12 good enough", Field.Store.YES);
//            StringField stringField2 = new StringField("content", " 招人，求私信 ", Field.Store.YES);
//
//            doc2.add(textField2);
//            doc2.add(stringField2);

            // index the document
            writer.addDocument(doc);
//            writer.addDocument(doc2);
            writer.commit();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
