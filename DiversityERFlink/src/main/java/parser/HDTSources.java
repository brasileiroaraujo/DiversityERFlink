package parser;
import java.io.IOException;

import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

public class HDTSources {
	private static HDT hdt;

	public static void main(String[] args) throws IOException, NotFoundException {
		HDT hdt = HDTManager.loadHDT("C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\wordnet31.hdt", null);
		//http://sws.geonames.org/2800482/ http://www.geonames.org/ontology#countryCode "BE"
//		IteratorTripleString it = hdt.search("", "", "\"FI\"");
		IteratorTripleString it = hdt.search("http://wordnet-rdf.princeton.edu/wn31/108797870-N", "", "");
		
		int count = 0;
	    while(it.hasNext()) {
	        TripleString ts = it.next();
	        count++;
	        
	        System.out.println(ts);//ts.getSubject());
	    }
	    System.out.println(count);
	 
	    // IMPORTANT: Close hdt when no longer needed
	    hdt.close();

	}
	
	public static IteratorTripleString loadHDT(String path) {
		IteratorTripleString triples = null;
		try {
			hdt = HDTManager.loadHDT(path, null);
			triples = hdt.search("", "", "");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return triples;
	}
	
	public static void close() {
		try {
			hdt.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
