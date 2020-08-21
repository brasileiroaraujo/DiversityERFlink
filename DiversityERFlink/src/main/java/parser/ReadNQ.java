package parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class ReadNQ {
	public static void main(String[] args) throws Exception {
		// We need to provide file path as the parameter:
		// double backquote is to avoid compiler interpret words
		// like \test as \t (ie. as a escape sequence)
		File file = new File("C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\all-geonames-rdf.txt");

		BufferedReader br = new BufferedReader(new FileReader(file));
		
		Model m = ModelFactory.createDefaultModel();

		String st;
		int i = 0;
		while ((st = br.readLine()) != null) {
			
			System.out.println(i + "- " + st/*.split("<").length*/);
			i++;
//			if (st.split("<").length > 5) {
//				break;
//			}
			if (i > 100) {
				break;
			}
		}
		System.out.println(m.size());
	}
}