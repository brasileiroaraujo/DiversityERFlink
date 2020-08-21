package parser;
import java.io.IOException;

import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

public class CompareResults {

	public static void main(String[] args) throws IOException, NotFoundException {
		HDT hdtWar = HDTManager.loadHDT("C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\warsampo\\municipalities.hdt", null);
		IteratorTripleString itWar = hdtWar.search("http://ldf.fi/warsa/places/municipalities/m_place_509", "", "");
		
		HDT hdtGeo = HDTManager.loadHDT("C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\geonames-11-11-2012.hdt", null);
		IteratorTripleString itGeo = hdtGeo.search("http://sws.geonames.org/658226/", "", ""); 
		
		
		HDT hdtNet = HDTManager.loadHDT("C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\wordnet31.hdt", null);
		IteratorTripleString itNet = hdtNet.search("http://wordnet-rdf.princeton.edu/wn31/108797870-N", "", "");
		
	    while(itGeo.hasNext()) {
	        TripleString ts = itGeo.next();
	        
	        System.out.println(ts);//ts.getSubject());
	    }
	    
	    System.out.println("--------------------");
	    
	    while(itWar.hasNext()) {
	        TripleString ts = itWar.next();
	        
	        System.out.println(ts);//ts.getSubject());
	    }
	    
	    System.out.println("--------------------");
	    
	    while(itNet.hasNext()) {
	        TripleString ts = itNet.next();
	        
	        System.out.println(ts);//ts.getSubject());
	    }
	 
	    hdtNet.close();
	    hdtGeo.close();
	    hdtWar.close();

	}
	

}
