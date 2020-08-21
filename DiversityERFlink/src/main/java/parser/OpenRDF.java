package parser;

import java.io.InputStream;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.FileManager;

public class OpenRDF {

	public static void main(String[] args) {
		Model model = ModelFactory.createDefaultModel();
		InputStream in = FileManager.get().open( "C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\\\Dictionaries (Words)\\\\geowordnet-rdf-20110330\\\\basic\\\\\\geowordnet-senselabels.rdf" );


		// read the RDF/XML file
		model.read(in, "RDF/XML");
		


	}

}
