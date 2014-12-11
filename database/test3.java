import java.util.Scanner;

import org.apache.jena.iri.impl.Main;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.CollectorStreamTriples;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.VCARD;


public class test3 {

	static String mmsi,name,lat,longi,callsign,speedoverground,courseoverground,rateOfturn,timestamp,navigationStatus,heading,imo,diamension,destination;
	static String msgdestination,msgappid,msgpayload,specialMano;
	
	public static void init()
	{
		mmsi=null;name=null;lat=null;longi=null;callsign=null;speedoverground=null;courseoverground=null;rateOfturn=null;timestamp=null;navigationStatus=null;
		heading=null;imo=null;diamension=null;destination=null;
		msgdestination=null;msgappid=null;msgpayload=null;specialMano=null;
	}

	
	public static void main(String[] args) {
//		// Create a model and read into it from file 
//		// "data.ttl" assumed to be Turtle.
//		Model model = RDFDataMgr.loadModel("D:/be project/furthertasks2/queries_n_data/ais_part0000_short.ttl") ;
//
//		// Create a dataset and read into it from file 
//		// "data.trig" assumed to be TriG.
//		Dataset dataset = RDFDataMgr.loadDataset("data.trig") ;
//
//		// Read into an existing Model
//		RDFDataMgr.read(model, "D:/be project/furthertasks2/queries_n_data/ais_part0000_short.ttl") ;
//		
		
		 final String filename = "data10.ttl";
		 
		 Scanner a =new Scanner(System.in);
	        
	        CollectorStreamTriples inputStream = new CollectorStreamTriples();
	        RDFDataMgr.parse(inputStream, filename);
	        
String msg = "default";
	        for (Triple triple : inputStream.getCollected()) {
	        
	        	if(msg.equals(triple.getSubject().toString()))
	        	{
	        		System.out.println(triple.getPredicate());
		        	System.out.println("--------------------------------");
		        	System.out.println(triple.getObject());
		        	System.out.println("==============================");

	        	}
	        	else if(!triple.getSubject().toString().contains("message"))
	        	{
	        		System.out.println(triple.getPredicate());
		        	System.out.println("--------------------------------");
		        	System.out.println(triple.getObject());
		        	System.out.println("==============================");
	        	}
	        	else 
	        	{
	        		msg=triple.getSubject().toString();
	        		System.out.println("[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[");
	        		//int w=a.nextInt();
	        		System.out.println(triple.getSubject());
		        	System.out.println("--------------------------------");
		        	//System.out.println(triple);
		        	//System.out.println("--------------------------------");
		        	System.out.println(triple.getPredicate());
		        	System.out.println("--------------------------------");
		        	System.out.println(triple.getObject());
		        	System.out.println("==================================");
	        		
	        	}
	        
	        	
//	        	Node msg=triple.getSubject();
//	        	System.out.println(triple.getSubject());
//	        	System.out.println("[[[[[[[[[[[[[[[[[[[[[[[[[[[");
//	        	while(triple.getSubject().equals(msg))
//	        	{
//	        		
//	        		System.out.println(triple);
//	        		System.out.println("--------------------------------");
//		        	System.out.println(triple.getPredicate());
//		        	System.out.println("--------------------------------");
//		        	System.out.println(triple.getObject());
//		       
//	        	}
//	        	System.out.println("[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[");
	        	
//	        	System.out.println(triple);
//	        	System.out.println("--------------------------------");
//	        	System.out.println(triple.getSubject());
//	        	System.out.println("--------------------------------");
//	        	System.out.println(triple.getPredicate());
//	        	System.out.println("--------------------------------");
//	        	System.out.println(triple.getObject());
//	        	System.out.println("====================================");
	        	
	        }
	}
	
	public static void analyse()
	{
		String pre,obj;
		pre="akash";
		switch(pre)
		{
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/mmsi":
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/callSign":
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/imo":
			
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/name":
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/destination":
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/dimensions":
			
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/navigationalStatus":		
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/speedOverGround":
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/courseOverGround":
			
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/timeStamp":
		case "http://www.w3.org/2003/01/geo/wgs84_pos#long":
		case "http://www.w3.org/2003/01/geo/wgs84_pos#lat":
			
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/heading":
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/specialManoeuvre":
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/rateOfTurn":
			
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/messageDestination":
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/messageApplicationID":
		case"http://semanticweb.cs.vu.nl/poseidon/ns/ais/typeLabel":
			
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/eta":
			
			
		}
	}
}
