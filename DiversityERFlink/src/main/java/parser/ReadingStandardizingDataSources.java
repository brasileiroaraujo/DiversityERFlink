package parser;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import Data.EntityProfile;


public class ReadingStandardizingDataSources {

	public static void main(String[] args) throws IOException, NotFoundException {
		List<EntityProfile> dataSource = new ArrayList<EntityProfile>();
		
		HDT hdt = HDTManager.loadHDT("C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\warsampo\\municipalities.hdt", null);
		String sourcePath = "C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\warsampo\\municipalities.hdt";
		String outputPath = "C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\warsampo\\municipalities_list";
		
		System.out.println("-----   LOADING HDT  -------");
		IteratorTripleString data = HDTSources.loadHDT(sourcePath);
		EntityProfile currentEntity = null;
		int count = 0;
		while(data.hasNext() /*&& count <= 1000*/) {
	        TripleString triple = data.next();
	        if (currentEntity == null || !triple.getSubject().toString().equals(currentEntity.getEntityUrl())) {
	        	if (currentEntity != null) {
					dataSource.add(currentEntity);
					count++;
				}
	        	currentEntity = new EntityProfile(triple.getSubject().toString());
	        	addInfo(hdt, currentEntity, triple);
			} else {
				addInfo(hdt, currentEntity, triple);
			}
	    }
		
//		for (EntityProfile e : dataSource.values()) {
//			Set<Property> linkedProperty = new HashSet<Property>();
//			for (Property url : e.getProperties()) {
//				EntityProfile eProp = dataSource.get(url.getValue().hashCode());
//				if (eProp != null) {
//					linkedProperty.add(new Property<EntityProfile>(url.getName(), eProp));
//				}
//			}
//			e.setProperties(linkedProperty);
//		}
		
		HDTSources.close();
		hdt.close();
		System.out.println("-----   HDT LOADED  -------");
		
		System.out.println("Size: " + dataSource.size());
		
		System.out.println("-----   STORING DATA  -------");
		//saving in object format
		FileOutputStream fos;
		ObjectOutputStream oos;
		try {
			fos = new FileOutputStream(outputPath);
			oos = new ObjectOutputStream(fos);
			oos.writeObject(dataSource);
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		

	}

	private static void addInfo(HDT hdt, EntityProfile currentEntity, TripleString triple) {
		if (triple.getObject().toString().contains("http")) {
			//adding the property
			IteratorTripleString it;
			try {//just execute if there is the linked entity (property) 
				it = hdt.search(triple.getObject().toString(), "", "");
				EntityProfile entityProperty = null;
				while(it.hasNext()) {
			        TripleString propertyTriple = it.next();
			        if (entityProperty == null) {
			        	entityProperty = new EntityProfile(propertyTriple.getSubject().toString());
					}
			        if (!propertyTriple.getObject().toString().contains("http")) {
			        	entityProperty.addAttribute(triple.getPredicate().toString(), triple.getObject().toString());
					}
			    }
				if (entityProperty != null && !entityProperty.getAttributes().isEmpty()) {//we just need useful entities (properties)
					currentEntity.addProperty(triple.getPredicate().toString(), entityProperty);
				}
			} catch (NotFoundException e) {
				//ignore
			}
			
		} else {
			currentEntity.addAttribute(triple.getPredicate().toString(), triple.getObject().toString());
		}
		
	}
	

}
