package com.test.jena;

import java.io.InputStream;
import java.util.HashMap;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.RDFReader;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.pfunction.library.concat;
import com.hp.hpl.jena.sparql.pfunction.library.container;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.RDF;

public class App 
{
	public static void main( String[] args )
	{
		System.out.println( "Hello World!" );


		try {
//			Location location = new Location ( "/Users/shri/devel/marine_data_project/data_files_test" );
//			Dataset dataset = TDBFactory.createDataset ( location );
//			dataset.begin ( ReadWrite.WRITE );
			
			String inputFileName = "/Users/shri/devel/marine_data_project/raw_files/ais_part0002.ttl";
//			String inputFileName = "/Users/shri/devel/marine_data_project/sample.ttl";

			// create an empty model
			Model model = ModelFactory.createDefaultModel();
			
			long t1 = System.currentTimeMillis();
			RDFReader bigFileReader = model.getReader("TTL");
			bigFileReader.setProperty("WARN_REDEFINITION_OF_ID","EM_IGNORE");

			// use the FileManager to find the input file
			InputStream in = FileManager.get().open( inputFileName );
			if (in == null) {
				throw new IllegalArgumentException(
						"File: " + inputFileName + " not found");
			}

			bigFileReader.read(model,in,null);
			System.out.println("time: " + (System.currentTimeMillis() - t1));

			System.out.println("Done");
			

			// get a single resource[
//			Resource res = model.getResource("http://example.org/message243902");
//			StmtIterator iter = res.listProperties();
//			while (iter.hasNext()) {
//			    Statement r = iter.nextStatement();
//			    System.out.println(r);
//			}
			
			
//			ResIterator iter = model.listStatements(new SimpleSelector(null, null, null));
			
			HashMap<String, Boolean> messageIDs = new HashMap<String, Boolean>();
			
			StmtIterator iter = model.listStatements();
			while (iter.hasNext()) {
				Statement r = iter.nextStatement();
				String subject = r.getSubject().toString();
				if(subject.contains("http://example.org/message")) {
					if(messageIDs.containsKey(subject)) {
						continue;
					}
					messageIDs.put(subject, true);
				}
			}
			
			System.out.println("--- MAP ----");
			for(String k : messageIDs.keySet()) {
				System.out.println(k);
			}
			
//			model.write(System.out);


		} catch (Exception e) {
			e.printStackTrace();
		}


	}
}
