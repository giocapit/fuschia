package lucenegiovva;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;


public class IndexerDoc {

	private final Metadata metadata;
	private final ContentHandler handler ;
    //System.out.println(handler.toString());
	private final ParseContext context ;
	private final Parser parser ;

   private final IndexWriter writer;

   public IndexerDoc(IndexWriter writer) {
		this.metadata =  new Metadata();
		this.handler = new BodyContentHandler(-1);
		this.context = new ParseContext();
		this.parser = new AutoDetectParser();

       this.writer = writer;
   }

	/** Indexes a single document 
	 * @throws TikaException */
	void indexDoc(Path file, long lastModified) throws IOException, TikaException {
			InputStream stream = Files.newInputStream(file);
			// make a new, empty document
			Document doc = new Document();
	
	        
	        //System.out.println(parser.getSupportedTypes(context));
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
			doc.add(new LongField("modified", lastModified, Field.Store.NO));
			//System.out.println(handler.toString());

			// Add the contents of the file to a field named "contents".  Specify a Reader,
			// so that the text of the file is tokenized and indexed, but not stored.
			// Note that FileReader expects the file to be in UTF-8 encoding.
			// If that's not the case searching for special characters will fail.
			doc.add(new TextField("contents", handler.toString(), Field.Store.NO));
			//doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

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
