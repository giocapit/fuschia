package test;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.fork.ForkParser;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class IndexerGiovvaTest {
	
//	static private Metadata metadata = new Metadata();
//    static private ContentHandler handler = new BodyContentHandler(-1);
//    //System.out.println(handler.toString());
    //static private ParseContext context = new ParseContext();
    static TikaConfig config ;
    //static private Parser parser;
//    static private DefaultDetector detector;
    static private ForkParser parser;

    //static private Tika tika = new Tika();

	private static int n = 0;
	private IndexerGiovvaTest() {}

	/** Index all text files under a directory. 
	 * @throws TikaException 
	 * @throws SAXException */
	public static void main(String[] args) throws SAXException, TikaException {
		String usage = "java lucenegiovva.IndexerGiovva"
				+ " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n"
				+ "This indexes the documents in DOCS_PATH, creating a Lucene index"
				+ "in INDEX_PATH that can be searched with SearchFiles";
		String indexPath = "index";
		String docsPath = null;
		boolean create = true;
		for(int i=0;i<args.length;i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i+1];
				i++;
			} else if ("-docs".equals(args[i])) {
				docsPath = args[i+1];
				i++;
			} else if ("-update".equals(args[i])) {
				create = false;
			}
		}

		if (docsPath == null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		final Path docDir = Paths.get(docsPath);
		if (!Files.isReadable(docDir)) {
			System.out.println("Document directory '" +docDir.toAbsolutePath()+ "' does not exist or is not readable, please check the path");
			System.exit(1);
		}

		Date start = new Date();
		try {
			System.out.println("Indexing to directory '" + indexPath + "'...");

			Directory dir = FSDirectory.open(Paths.get(indexPath));
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

			if (create) {
				// Create a new index in the directory, removing any
				// previously indexed documents:
				iwc.setOpenMode(OpenMode.CREATE);
			} else {
				// Add new documents to an existing index:
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);

			}

			// Optional: for better indexing performance, if you
			// are indexing many documents, increase the RAM
			// buffer.  But if you do this, increase the max heap
			// size to the JVM (eg add -Xmx512m or -Xmx1g):
			//
			iwc.setRAMBufferSizeMB(256.0);

			IndexWriter writer = new IndexWriter(dir, iwc);
			int m = 0;
			while(m<30){
			if (create){
				indexDocs(writer, docDir);
			} else {
				IndexReader reader = DirectoryReader.open(dir);
				IndexSearcher searcher = new IndexSearcher(reader);
				indexDocsUpdate(writer,searcher, docDir);
			}
			m++;
			}

			// NOTE: if you want to maximize search performance,
			// you can optionally call forceMerge here.  This can be
			// a terribly costly operation, so generally it's only
			// worth it when your index is relatively static (ie
			// you're done adding documents to it):
			//
			// writer.forceMerge(1);

			writer.close();

			Date end = new Date();
			System.out.println(end.getTime() - start.getTime() + " total milliseconds");

		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() +
					"\n with message: " + e.getMessage());
		}
	}

	static void indexDocsUpdate(final IndexWriter writer, final IndexSearcher searcher,/* final QueryParser qparser,*/ Path path) throws IOException, SAXException, TikaException {
		if (Files.isDirectory(path)) {
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					try {

						String line = new String(file.toString());
						line.trim();
						Term term = new Term("path",line);
						TermQuery tquery = new TermQuery(term);
						//Query query = qparser.parse(line);
						//qparser.setAllowLeadingWildcard(true);
						//System.out.println(file.toString());
						TopDocs results= searcher.search(tquery, 10);
						ScoreDoc[] hits = results.scoreDocs;
						int numTotalHits = results.totalHits;
						if(numTotalHits > 0){
							Document doc = searcher.doc(hits[0].doc);
							FileTime docLastMod = FileTime.from(new Long(doc.get("modified")), TimeUnit.SECONDS);
							if(docLastMod.compareTo(attrs.lastModifiedTime())<0){
								indexDoc(writer,  file, attrs.lastModifiedTime().to(TimeUnit.SECONDS));
							}
						
						} else indexDoc(writer,  file, attrs.lastModifiedTime().to(TimeUnit.SECONDS));
					} catch (IOException ignore) {
						// don't index files that can't be read.
					} catch (SAXException e) {
						e.printStackTrace();
					} catch (TikaException e) {
						e.printStackTrace();
					} /*catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}*/
					return FileVisitResult.CONTINUE;
				}
			});
		} else {
			indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
		}
		writer.commit();
		writer.deleteUnusedFiles();
		
		System.out.println(writer.maxDoc() + " documents written");
	}


	/**
	 * Indexes the given file using the given writer, or if a directory is given,
	 * recurses over files and directories found under the given directory.
	 * 
	 * NOTE: This method indexes one document per input file.  This is slow.  For good
	 * throughput, put multiple documents into your input file(s).  An example of this is
	 * in the benchmark module, which can create "line doc" files, one document per line,
	 * using the
	 * <a href="../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
	 * >WriteLineDocTask</a>.
	 *  
	 * @param writer Writer to the index where the given file/dir info will be stored
	 * @param path The file to index, or the directory to recurse into to find files to index
	 * @throws IOException If there is a low-level I/O error
	 * @throws TikaException 
	 * @throws SAXException 
	 */
	static void indexDocs(final IndexWriter writer, Path path) throws IOException, SAXException, TikaException {
		if (Files.isDirectory(path)) {
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					try {
					    config = new TikaConfig("/home/giovanni/tika-config.xml");
					    //parser = new AutoDetectParser(config);
					    //detector = new DefaultDetector();
					    parser = new ForkParser(IndexerGiovvaTest.class.getClassLoader(),new AutoDetectParser());

						n++;
						indexDoc(writer, file, attrs.lastModifiedTime().to(TimeUnit.SECONDS));
						if(n % 100 == 0)
							writer.commit();
					} catch (IOException e) {
						e.printStackTrace();
						// don't index files that can't be read.
					} catch (SAXException e) {
						e.printStackTrace();
					} catch (TikaException e) {
						e.printStackTrace();
					}
					return FileVisitResult.CONTINUE;
				}
			});
		} else {
			indexDoc(writer, path, Files.getLastModifiedTime(path).to(TimeUnit.SECONDS));
		}
		writer.commit();
		writer.deleteUnusedFiles();
		
		System.out.println(writer.maxDoc() + " documents written");
	}

	/** Indexes a single document 
	 * @throws TikaException 
	 * @throws SAXException */
	static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException, SAXException, TikaException {
		try (InputStream stream = Files.newInputStream(file)) {
			// make a new, empty document
			Document doc = new Document();
			
			Metadata metadata = new Metadata();
			BodyContentHandler handler = new BodyContentHandler(-1);
			//PipedInputStream inStream = new PipedInputStream(4096);
		    ///PipedOutputStream outStream = new PipedOutputStream(inStream);
		    
		    /*
			DocumentWriterTest docWriter = new DocumentWriterTest(inStream,doc);
			Thread documentWriterThread = new Thread(docWriter);
			documentWriterThread.start();
			*/
		    //System.out.println(handler.toString());
		    ParseContext context = new ParseContext();
		    
	        try {
	        	parser.parse(stream, handler, metadata, context);
	        	//System.out.println(handler.toString());
	        }catch(TikaException e){
	        	e.printStackTrace();
	        }catch(SAXException e){
	        	e.printStackTrace();
	        }finally{
	        
	        	stream.close();
	        }


	        //System.out.println(parser.getSupportedTypes(context));
		    /*new Thread(
		    		new DocumentParser(stream, parser, outStream,context)
		    		).start();*/


			/*try {
				documentWriterThread.join();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}*/

			//ParsingReader reader = new ParsingReader(parser, stream, metadata, context);
			
			
			// Add the path of the file as a field named "path".  Use a
			// field that is indexed (i.e. searchable), but don't tokenize 
			// the field into separate words and don't index term frequency
			// or positional information:
			Field pathField = new StringField("path", file.toString(), Field.Store.YES);
			doc.add(pathField);

			// Add the last modified date of the file a field named "modified".
			// Use a LongField that is indexed (i.e. efficiently filterable with
			// NumericRangeFilter).  This indexes to milli-second resolution, which
			// is often too fine.  You could instead create a number based on
			// year/month/day/hour/minutes/seconds, down the resolution you require.
			// For example the long value 2011021714 would mean
			// February 17, 2011, 2-3 PM.
			doc.add(new LongField("modified", lastModified, Field.Store.YES));
			//System.out.println(handler.toString());

			// Add the contents of the file to a field named "contents".  Specify a Reader,
			// so that the text of the file is tokenized and indexed, but not stored.
			// Note that FileReader expects the file to be in UTF-8 encoding.
			// If that's not the case searching for special characters will fail.
			
			/*try{
				doc.add(new TextField("contents", tika.parseToString(stream,metadata),Field.Store.NO));
			}finally{
			stream.close();
			}*/
			doc.add(new TextField("contents", handler.toString(), Field.Store.NO));
			//doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(inStream, StandardCharsets.UTF_8))));
			//doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

			PDFont.clearResources();
			if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
				// New index, so we just add the document (no old document can be there):
				System.out.println("adding " + file);
				writer.addDocument(doc);
			} else {
				// Existing index (an old copy of this document may have been indexed) so 
				// we use updateDocument instead to replace the old one matching the exact 
				// path, if present:
				System.out.println("updating " + file);
				writer.updateDocument(new Term("path", file.toString()), doc);
			}
			
		}
	}
}
