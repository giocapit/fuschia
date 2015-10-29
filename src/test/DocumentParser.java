package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class DocumentParser implements Runnable{

	private InputStream stream;
	private Parser parser;
	private ParseContext context;
	private PipedOutputStream outStream;
	
	
	public DocumentParser(InputStream inStream,Parser parser, PipedOutputStream outStream, ParseContext context){
		this.stream = inStream;
		this.parser = parser;
		this.outStream  = outStream;
		this.context = context;
	}
	@Override
	public void run() {
		Metadata metadata = new Metadata();
		ContentHandler handler = new BodyContentHandler(outStream);
		//doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(inStream, StandardCharsets.UTF_8))));
        try
        {
	        try {
	        	parser.parse(stream, handler, metadata, context);
	        	//MediaType mimetype = detector.detect(new BufferedInputStream(stream), metadata);
	        	//System.out.println(mimetype);

	        	//System.out.println(handler.toString());
	        }catch(TikaException e){
	        	e.printStackTrace();
	        }catch(SAXException e){
	        	e.printStackTrace();
	        }finally{
	        
	        	stream.close();
                outStream.flush();
                outStream.close();

	        }

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }




}
