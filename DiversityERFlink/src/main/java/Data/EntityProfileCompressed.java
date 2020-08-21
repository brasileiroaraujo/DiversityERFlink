package Data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 *
 * @author gap2
 */

public class EntityProfileCompressed implements Serializable, Comparable<EntityProfileCompressed> {

	private static final long serialVersionUID = 122354534453243447L;

	private Set<Integer> tokens;
	private Set<Integer> tokensOfProperties;
	private String entityUrl;
	private int sourceID;
	private double similarity;
	private double diversity;
//	private Map<Integer, Tuple2<Integer, Double>> diversityBetweenNeighbors;
	
	
	public EntityProfileCompressed(EntityProfile fullEntity) {
		super();
		this.entityUrl = fullEntity.getEntityUrl();
		this.sourceID = fullEntity.getSourceID();
		this.tokens = fullEntity.generateTokens();
		this.tokensOfProperties = extractTokens(fullEntity.getProperties());
		
	}
	
	private Set<Integer> extractTokens(Set<Property> properties) {
		Set<Integer> tokensFromProperties = new HashSet<Integer>();
		for (Property<EntityProfile> entity : properties) {
			tokensFromProperties.addAll(entity.getValue().generateTokens());
		}
		return tokensFromProperties;
	}

	public int getKey() {
		return getEntityUrl().hashCode();
	}

	
	public String getEntityUrl() {
		return entityUrl;
	}

	public Set<Integer> getTokens() {
		return tokens;
	}


	public void setTokens(Set<Integer> newTokens) {
		this.tokens = newTokens;
	}
	

	public int getSourceID() {
		return sourceID;
	}


	public void setSource(int sourceID) {
		this.sourceID = sourceID;
	}
	
	
	public Set<Integer> getTokensOfProperties() {
		return tokensOfProperties;
	}

	public void setTokensOfProperties(Set<Integer> tokensOfProperties) {
		this.tokensOfProperties = tokensOfProperties;
	}

	public double getSimilarity() {
		return similarity;
	}

	public void setSimilarity(double similarity) {
		this.similarity = similarity;
	}

	public double getDiversity() {
		return diversity;
	}

	public void setDiversity(double diversity) {
		this.diversity = diversity;
	}

//	public Map<Integer, Tuple2<Integer, Double>> getDiversityBetweenNeighbors() {
//		return diversityBetweenNeighbors;
//	}
//
//	public void setDiversityBetweenNeighbors(Map<Integer, Tuple2<Integer, Double>> diversityBetweenNeighbors) {
//		this.diversityBetweenNeighbors = diversityBetweenNeighbors;
//	}

	@Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final EntityProfileCompressed other = (EntityProfileCompressed) obj;
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
	public String toString() {
		// TODO Auto-generated method stub
		return getEntityUrl() + " (" + sourceID + "-" + similarity + "-" + diversity + ") ";
	}


	@Override
	public int compareTo(EntityProfileCompressed other) {
//		if (this.getDiversity() <= other.getDiversity()) {
//			return 1;
//		} else {
//			return -1;
//		}
		if (this.getSimilarity() <= other.getSimilarity()) {
			return 1;
		} else {
			return -1;
		}
	}


}