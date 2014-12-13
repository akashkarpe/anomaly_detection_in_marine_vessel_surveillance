import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.CollectorStreamTriples;

import com.hp.hpl.jena.graph.Triple;



public class insert_database {

	static String mmsi,name,lat,longi,callsign,speedoverground,courseoverground,rateOfturn,timestamp,navigationStatus,heading,imo,dimension,destination;
	static String msgdestination,msgappid,msgpayload,specialMano,eta,typelable,actortype;
	
	static Connection con;
	static Statement stmt;
	static ResultSet rs;
	
 public static void init_database() throws ClassNotFoundException, SQLException
	{
	 Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
		con=DriverManager.getConnection("jdbc:odbc:aks","root","tiger");
		stmt=con.createStatement();
	}
	
	public static void init()
	{
		mmsi=null;name=null;lat=null;longi=null;callsign=null;speedoverground=null;courseoverground=null;rateOfturn=null;
		timestamp=null;navigationStatus=null;
		heading=null;imo=null;dimension=null;destination=null;
		msgdestination=null;msgappid=null;msgpayload=null;specialMano=null;
		eta=null;typelable=null;actortype=null;
	}
public static void display()
{
	System.out.println("mmsi="+mmsi);
	System.out.println("name="+name);
	System.out.println("lat="+lat);
	System.out.println("long="+longi);
	System.out.println("callsign="+callsign);
	System.out.println("speedoverground="+speedoverground);
	System.out.println("courseoverground="+courseoverground);
	System.out.println("rateOfturn="+rateOfturn);
	System.out.println("timestamp="+timestamp);
	System.out.println("navigationStatus="+navigationStatus);
	System.out.println("heading="+heading);
	System.out.println("imo="+imo);
	System.out.println("dimension="+dimension);
	System.out.println("destination="+destination);
	System.out.println("msgdestination="+msgdestination);
	System.out.println("msgappid="+msgappid);
	System.out.println("msgpayload="+msgpayload);
	System.out.println("specialMano="+specialMano);
	System.out.println("eta="+eta);
	System.out.println("typelable="+typelable);
	System.out.println("actortype="+actortype);
	
}
	

public static void insert() throws SQLException
{
	int i=stmt.executeUpdate("insert into demo values('"+mmsi+"','"+name+"','"+lat+"','"+longi+"','"+callsign+"','"+speedoverground+"','"+courseoverground+"','"+rateOfturn+"','"+timestamp+"','"+navigationStatus+"','"+heading+"','"+imo+"','"+dimension+"','"+destination+"','"+msgdestination+"','"+msgappid+"','"+msgpayload+"','"+specialMano+"','"+eta+"','"+typelable+"','"+actortype+"')");
	if(i>0)
	{
		System.out.println("inserted successfully");
		
	}
	else
		System.out.println("unsuccessfully");
		
	
}



	public static void main(String[] args) throws ClassNotFoundException, SQLException {
	
		
		init_database();
		
		
		
		 final String filename = "data.ttl";
		 
		 Scanner a =new Scanner(System.in);
	        
	        CollectorStreamTriples inputStream = new CollectorStreamTriples();
	        RDFDataMgr.parse(inputStream, filename);
	        
String msg = "default";
String predicate;
String object;
	        for (Triple triple : inputStream.getCollected()) {
	        
	        	if(msg.equals(triple.getSubject().toString()))
	        	{

		        	predicate=triple.getPredicate().toString();
	        		object=triple.getObject().toString();
	        		analyse(predicate,object);

	        	}
	        	else if(!triple.getSubject().toString().contains("message"))
	        	{
		        	predicate=triple.getPredicate().toString();
	        		object=triple.getObject().toString();
	        		analyse(predicate,object);
	        	}
	        	else 
	        	{
	        		insert();
	        		//display();
	        		//int q=a.nextInt();
	        		msg=triple.getSubject().toString();
	        		init();

	        		predicate=triple.getPredicate().toString();
	        		object=triple.getObject().toString();
	        		analyse(predicate,object);
	        	}
	        
	        	

	        
	        }
	}
	
	public static void analyse(String predicate,String object)
	{int i = 0,j=0;
	
	
	Scanner a =new Scanner(System.in);
	Pattern pattern;
	Matcher matcher;
	String obj1;
		switch(predicate)
		{
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/mmsi":
			 pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
		  obj1=matcher.group(1).toString();
		  
			mmsi=obj1;
		
				break;
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/callSign":
			 pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			 obj1=matcher.group(1).toString();
			callsign=obj1;
			
			break;
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/imo":
			 pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			
			 obj1=matcher.group(1).toString();
			imo=obj1;
			
			
			break;
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/name":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			
			 obj1=matcher.group(1).toString();
			name=obj1;
		
			break;
			
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/destination":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			
			 obj1=matcher.group(1).toString();
			destination=obj1;
			
			break;
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/dimensions":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			 obj1=matcher.group(1).toString();
			dimension=obj1;
			
			break;
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/navigationalStatus":
			pattern = Pattern.compile("http://semanticweb.cs.vu.nl/poseidon/ns/ais/(.*)");
			 matcher = pattern.matcher(object);
			
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			 obj1=matcher.group(1).toString();
			navigationStatus=obj1;
			
			break;
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/speedOverGround":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			 obj1=matcher.group(1).toString();
			speedoverground=obj1;
			
			break;
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/courseOverGround":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			
			 obj1=matcher.group(1).toString();
			courseoverground=obj1;
			
			break;
			
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/timeStamp":
			
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			
			 obj1=matcher.group(1).toString();
			timestamp=obj1;
			
			break;
		case "http://www.w3.org/2003/01/geo/wgs84_pos#long":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			
			 obj1=matcher.group(1).toString();
			longi=obj1;
			
			break;
		case "http://www.w3.org/2003/01/geo/wgs84_pos#lat":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			
			 obj1=matcher.group(1).toString();
			lat=obj1;
			
			break;
			
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/heading":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			 obj1=matcher.group(1).toString();
			heading=obj1;
			
			break;
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/specialManoeuvre":
			pattern = Pattern.compile("http://semanticweb.cs.vu.nl/poseidon/ns/ais/(.*)");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			
			 obj1=matcher.group(1).toString();
			specialMano=obj1;
			
			
			break;
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/rateOfTurn":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			
			 obj1=matcher.group(1).toString();
			rateOfturn=obj1;
			
			break;
			
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/messageDestination":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			
			 obj1=matcher.group(1).toString();
			msgdestination=obj1;
			
			break;
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/messageApplicationID":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			
			 obj1=matcher.group(1).toString();
			msgappid=obj1;
			
			break;
		case"http://semanticweb.cs.vu.nl/poseidon/ns/ais/typeLabel":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			 obj1=matcher.group(1).toString();
			
			break;
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/messagePayload":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			
			 obj1=matcher.group(1).toString();
			msgpayload=obj1;
			
			break;
		case "http://semanticweb.cs.vu.nl/poseidon/ns/ais/eta":
			pattern = Pattern.compile("\"(.*?)\"");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				}
			
			 obj1=matcher.group(1).toString();
			eta=obj1;
			
			break;
		case"http://semanticweb.cs.vu.nl/2009/11/sem/actorType":
			pattern = Pattern.compile("http://semanticweb.cs.vu.nl/poseidon/ns/instances/(.*)");
			 matcher = pattern.matcher(object);
			 if (matcher.find())
				{
				   // System.out.println(matcher.group(1));
				 obj1=matcher.group(1).toString();
					specialMano=obj1;
				}
		
			
			
			break;
			
		}
	}
}
