package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;

public class DocumentWriterTest implements Runnable{

	private InputStream inStream;
	private Document doc;
	
	public DocumentWriterTest(InputStream inStream,Document doc){
		this.inStream = inStream;
		this.doc = doc;
	}
	@Override
	public void run() {
		
		//doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(inStream, StandardCharsets.UTF_8))));
        try
        {
            BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( inStream ));
            String temp=null;
            while((temp=bufferedReader.readLine())!=null)
            {
                System.out.println(temp);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }




}
