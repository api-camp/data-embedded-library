package com.de314.data.local.api.search;

import com.de314.data.local.utils.FileUtils;
import lombok.Builder;
import lombok.Value;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Demo {

    public static void main(String[] args) throws IOException, ParseException {
        String namespace = "demo";
        File searchDir = new File(getPath(namespace).toString());
        if (searchDir.exists()) {
            FileUtils.delete(searchDir.getPath());
        }

        try (SearchStoreContext ctx = boostrap("demo")) {

            writeDocuments(ctx);
            ctx.getIndexWriter().close();

            QueryParser titleQueryParser = new QueryParser("title", ctx.getAnalyzer());
            QueryParser bodyQueryParser = new QueryParser("body", ctx.getAnalyzer());

            System.out.println("Good Results");
            List<Document> results = search(ctx, titleQueryParser, "gOOd");
            results.forEach(doc -> System.out.println("  - id=" + doc.get("id")));

            System.out.println("Car Results");
            results = search(ctx, bodyQueryParser, "car OR drive");
            results.forEach(doc -> System.out.println("  - id=" + doc.get("id")));

            System.out.println("Mispelled Results");
            results = fuzzySearch(ctx, "body", "furd");
            results.forEach(doc -> System.out.println("  - id=" + doc.get("id")));

            Query
        }
    }

    private static Path getPath(String namespace) {
        return Paths.get("data", "search", namespace);
    }

    private static SearchStoreContext boostrap(String namespace) throws IOException {
        Directory memoryIndex = new MMapDirectory(getPath(namespace));
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        IndexWriter indexWriter = new IndexWriter(memoryIndex, indexWriterConfig);

        return SearchStoreContext.builder()
                .index(memoryIndex)
                .analyzer(analyzer)
                .indexWriter(indexWriter)
                .build();
    }

    private static void writeDocuments(SearchStoreContext ctx) throws IOException {
        long id = 0;
        write(ctx, id++,
                "Ford v Ferrari",
                "American car designer Carroll Shelby and driver Ken Miles battle corporate interference, the laws of physics and their own personal demons to build a revolutionary race car for Ford and challenge Ferrari at the 24 Hours of Le Mans in 1966.");
        write(ctx, id++,
                "The Good Liar",
                "Consummate con man Roy Courtnay has set his sights on his latest mark: the recently widowed Betty McLeish, worth millions. But this time, what should have been a simple swindle escalates into a cat-and-mouse game with the ultimate stakes.");
        write(ctx, id++,
                "The Warrior Queen of Jhansi",
                "A tale of women's empowerment, The Warrior Queen of Jhansi tells the true story of Lakshmibai, the historic Queen of Jhansi who fiercely led her army against the British East India Company in the infamous mutiny of 1857.");
        write(ctx, id++,
                "Charlie's Angels",
                "When a young systems engineer blows the whistle on a dangerous technology, Charlie's Angels are called into action, putting their lives on the line to protect us all.");
    }

    private static void write(SearchStoreContext ctx, long id, String title, String body) throws IOException {
        Document doc = new Document();

        doc.add(new TextField("id", id + "", Field.Store.YES));
        doc.add(new TextField("title", title, Field.Store.NO));
        doc.add(new TextField("body", body, Field.Store.NO));

        ctx.getIndexWriter().addDocument(doc);
    }

    private static List<Document> search(SearchStoreContext ctx, QueryParser queryParser, String queryString) throws ParseException, IOException {
        Query query = queryParser.parse(queryString);
        return search(ctx, query);
    }

    private static List<Document> fuzzySearch(SearchStoreContext ctx, String inField, String termString) throws ParseException, IOException {
        FuzzyQuery query = new FuzzyQuery(new Term(inField, termString), 1, 1);
        return search(ctx, query);
    }

    private static List<Document> search(SearchStoreContext ctx, Query query) throws ParseException, IOException {
        IndexReader indexReader = DirectoryReader.open(ctx.getIndex());
        IndexSearcher searcher = new IndexSearcher(indexReader);

        TopDocs topDocs = searcher.search(query, 10);

        List<Document> documents = new ArrayList<>();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            documents.add(searcher.doc(scoreDoc.doc));
        }

        return documents;
    }

    @Value
    @Builder(toBuilder = true)
    private static class SearchStoreContext implements AutoCloseable {
        private Directory index;
        private Analyzer analyzer;
        private IndexWriter indexWriter;

        public IndexSearcher getSearcher() {
            try {
                return new IndexSearcher(DirectoryReader.open(this.index));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        public void close() {
            try {
//                indexWriter.close();
                index.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
