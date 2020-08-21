package parser;
import java.io.IOException;

import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;

public class RDF2HDT {

	public static void main(String[] args) throws IOException, ParserException {
		// Configuration variables
        String baseURI = "http://www.ldf.fi/dataset/warsa";
        String rdfInput = "C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\warsampo\\municipalities_production.ttl";
        String hdtOutput = "C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\warsampo\\municipalities.hdt";
 
        // Create HDT from RDF file
        HDT hdt = HDTManager.generateHDT(
                            rdfInput,         // Input RDF File
                            baseURI,          // Base URI
                            RDFNotation.TURTLE, // Input Type
                            new HDTSpecification(),   // HDT Options
                            null              // Progress Listener
                );
 
        // OPTIONAL: Add additional domain-specific properties to the header:
        //Header header = hdt.getHeader();
        //header.insert("myResource1", "property" , "value");
 
        // Save generated HDT to a file
        hdt.saveToHDT(hdtOutput, null);
 
        // IMPORTANT: Close hdt when no longer needed
        hdt.close();

	}

}
