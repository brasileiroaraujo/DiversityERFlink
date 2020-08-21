package parser;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.List;

import Data.EntityProfile;

public class LoadDataSource {

	public static void main(String[] args) {
		for (EntityProfile e : loadData("C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\wordnet31_list_locations")) {
//			if (e.getEntityUrl().equalsIgnoreCase("http://sws.geonames.org/647867/") || e.getEntityUrl().equalsIgnoreCase("http://sws.geonames.org/647873/") || e.getEntityUrl().equalsIgnoreCase("http://sws.geonames.org/647874/"))
			if (e.getEntityUrl().equalsIgnoreCase("http://wordnet-rdf.princeton.edu/wn31/108797870-N"))
			System.out.println(e.printAttributes());
		}
		
		System.out.println("----------------");
		
//		for (EntityProfile e : loadData("C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\warsampo\\places_list")) {
//			if (e.getEntityUrl().equalsIgnoreCase("http://ldf.fi/warsa/places/karelian_places/k_place_17479"))
//				System.out.println(e.getEntityUrl() + "-" + e.printAttributes());
//		}
		

	}
	
	public static List<EntityProfile> loadData(String path) {
		FileInputStream fis;
		ObjectInputStream ois;
		List<EntityProfile> entities = null;
		try {
			fis = new FileInputStream(path);
			ois = new ObjectInputStream(fis);
			entities = (List<EntityProfile>) ois.readObject();
			ois.close();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		
		return entities;
	}

}
