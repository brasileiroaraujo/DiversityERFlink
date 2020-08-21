/*
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    Copyright (C) 2015 George Antony Papadakis (gpapadis@yahoo.gr)
 */

package Data;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

/**
 *
 * @author gap2
 */

public class EntityProfile implements Serializable, Comparable<EntityProfile> {

	private static final long serialVersionUID = 122354534453243447L;

	private Set<Attribute> attributes;
	private Set<Property> properties;
	private Set<Integer> tokens;
	private Set<EntityProfile> neighbors;
	private String entityUrl;
	private int sourceID;
	private double similarity = 0.0;
	private double diversity = 0.0;
	
	
	public EntityProfile(String urlEntity) {
		super();
		entityUrl = urlEntity;
		this.attributes = new HashSet<Attribute>();
		this.properties = new HashSet<Property>();
		this.neighbors = new HashSet<EntityProfile>();
	}
	
	
//	public void addInfo(TripleString triple) {
//		if (triple.getObject().toString().contains("http")) {
//			properties.add(new Property<String>(triple.getPredicate().toString(), triple.getObject().toString()));
//		} else {
//			attributes.add(new Attribute(triple.getPredicate().toString(), triple.getObject().toString()));
//		}
//	}

	public int getKey() {
		return getEntityUrl().hashCode();
	}

	public void addAttribute(String propertyName, String propertyValue) {
		attributes.add(new Attribute(propertyName, propertyValue));
	}
	
	public void addProperty(String propertyName, EntityProfile property) {
		properties.add(new Property<EntityProfile>(propertyName, property));
	}
	
	public void addNeighbor(EntityProfile e) {
		neighbors.add(e);
	}
	
	public void addAllNeighbor(Collection<EntityProfile> entities) {
		neighbors.addAll(entities);
	}


	public Set<EntityProfile> getNeighbors() {
		return neighbors;
	}


	public void setNeighbors(Set<EntityProfile> neighbors) {
		this.neighbors = neighbors;
	}


	public String getEntityUrl() {
		return entityUrl;
	}

	public int getProfileSize() {
		return attributes.size();
	}

	public Set<Attribute> getAttributes() {
		return attributes;
	}
	
	public Set<Integer> getTokens() {
		return tokens;
	}


	public void setTokens(Set<Integer> newTokens) {
		this.tokens = newTokens;
	}
	
	public double getSimilarity() {
		return similarity;
	}


	public void setSimilarity(double similarity) {
		this.similarity = similarity;
	}
	
//	public String getStandardFormat() {
//		String output = "";
//		output += isSource + split1;
//		output += entityUrl + split1;//separate the attributes
//		output += key + split1;//separate the attributes
//		
//		for (Attribute attribute : attributes) {
//			output += attribute.getName() + split2 + attribute.getValue() + split1;
//		}
//				
//		return output;
//	}


	public double getDiversity() {
		return diversity;
	}


	public void setDiversity(double diversity) {
		this.diversity = diversity;
	}


	public Set<Property> getProperties() {
		return properties;
	}


	public void setProperties(Set<Property> properties) {
		this.properties = properties;
	}


	public int getSourceID() {
		return sourceID;
	}


	public void setSource(int sourceID) {
		this.sourceID = sourceID;
	}
	
	public Set<Integer> generateTokens() {
		tokens = new HashSet<Integer>();
		KeywordGenerator kw = new KeywordGeneratorImpl();
		Pattern p = Pattern.compile("[^\"a-zA-Z\\s0-9]");
		 
		
		for (Attribute attribute : attributes) {
			//To improve quality, use the following code
			Matcher m = p.matcher("");
			m.reset(attribute.getValue());
			String standardString = m.replaceAll("");
			
			tokens.addAll(kw.generateKeyWordsHashCode(standardString));
		}
		
		return tokens;
		
	}
	
	@Override
	public String toString() {
		String attributeList = "";
		for (EntityProfile n : neighbors) {
			attributeList += n.getEntityUrl() + "(" + n.getSourceID() + " - " + n.getSimilarity()  + " - " + n.getDiversity() +")" + ", ";
		}
		return entityUrl + "(" + getSourceID() +")" + ": " + (attributeList.isEmpty() ? "" : attributeList.substring(0, attributeList.length()-2));
	}
	
	public String printProperties() {
		String propertyList = "";
		for (Property<EntityProfile> p : properties) {
			propertyList += p.getValue().getEntityUrl() + ", ";
		}
		return entityUrl + "(" + getSourceID() +")" + ": " + (propertyList.isEmpty() ? "" : propertyList.substring(0, propertyList.length()-2));
	}
	
	
	public String printAttributes() {
		String attributeList = "";
		for (Attribute a : attributes) {
			attributeList += a.getValue() + ", ";
		}
		return entityUrl + "(" + getSourceID() +")" + ": " + (attributeList.isEmpty() ? "" : attributeList.substring(0, attributeList.length()-2));
	}
	
	@Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final EntityProfile other = (EntityProfile) obj;
        if (this.getKey() != other.getKey()) {
            return false;
        }

        return true;
    }
	
	@Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + (this.entityUrl != null ? this.entityUrl.hashCode() : 0);
        return hash;
    }


	@Override
	public int compareTo(EntityProfile other) {
		if (this.getDiversity() <= other.getDiversity()) {
			return 1;
		} else {
			return -1;
		}
		
//		if (this.getSimilarity() <= other.getSimilarity()) {
//			return 1;
//		} else {
//			return -1;
//		}
	}


	


}