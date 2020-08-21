package compressed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import Data.EntityProfileCompressed;
import Data.TreeSetBag;

public class EntityReduceCompressed implements GroupReduceFunction<Tuple2<Integer, Tuple2<EntityProfileCompressed, EntityProfileCompressed>>, Tuple2<String, Set<EntityProfileCompressed>>> {
	private boolean evaluateDiversityBetweenNeighbors;
	private int numberOfPairsPerEntity;
	
	public EntityReduceCompressed(boolean flagDiversityBetweenNeighbors, int numberOfPairs) {
		this.evaluateDiversityBetweenNeighbors = flagDiversityBetweenNeighbors;
		this.numberOfPairsPerEntity = numberOfPairs;
	}

	@Override
	public void reduce(Iterable<Tuple2<Integer, Tuple2<EntityProfileCompressed, EntityProfileCompressed>>> values,
			Collector<Tuple2<String, Set<EntityProfileCompressed>>> out) throws Exception {
		
		//comparing entities
		double similarityTotalDS2 = 0;
		double similarityTotalDS3 = 0;
		int numberOfNeighborsDS2 = 0;
		int numberOfNeighborsDS3 = 0;
		
		EntityProfileCompressed current = null;
		Set<EntityProfileCompressed> allEntitiesFromDS2 = new HashSet<EntityProfileCompressed>(); //it needed because Iterables can be iterated over only once.
		Set<EntityProfileCompressed> allEntitiesFromDS3 = new HashSet<EntityProfileCompressed>();
		Map<EntityProfileCompressed, Set<Tuple2<Integer, Double>>> diversityBetweenNeighbors = new HashMap<EntityProfileCompressed, Set<Tuple2<Integer, Double>>>();
		
		for (Tuple2<Integer, Tuple2<EntityProfileCompressed, EntityProfileCompressed>> tuple : values) {
			if (current == null) {
				current = tuple.f1.f0; //entity from source
			}
			
			//the evaluation is performed individually in each DS, to avoid unfair pruning
			boolean result;
			if (tuple.f1.f1.getSourceID() == 2) {
				result = allEntitiesFromDS2.add(tuple.f1.f1);
				if (result) {//just count if it does not have duplicates
					similarityTotalDS2 += tuple.f1.f1.getSimilarity(); //entity from target bases
					numberOfNeighborsDS2 += 1;
					if (evaluateDiversityBetweenNeighbors) {
						calculateDiverstityBetweenNeighbors(tuple.f1.f1, diversityBetweenNeighbors);
					}
					
				}
			} else {
				result = allEntitiesFromDS3.add(tuple.f1.f1);
				if (result) {//just count if it does not have duplicates
					similarityTotalDS3 += tuple.f1.f1.getSimilarity(); //entity from target bases
					numberOfNeighborsDS3 += 1;
					
					if (evaluateDiversityBetweenNeighbors) {
						calculateDiverstityBetweenNeighbors(tuple.f1.f1, diversityBetweenNeighbors);
					}
				}
			}
			
		}
		
		double similarityAverageDS2 = similarityTotalDS2/numberOfNeighborsDS2;
		double similarityAverageDS3 = similarityTotalDS3/numberOfNeighborsDS3;
		
		//performing WNP algorithm applying diversity criterion
		//for DS2 entities
		TreeSetBag<EntityProfileCompressed> prunedNeighbors = new TreeSetBag<EntityProfileCompressed>(numberOfPairsPerEntity); //this structure performs the ranking
		for (EntityProfileCompressed entity : allEntitiesFromDS2) {
			if (entity.getSimilarity() >= similarityAverageDS2) {
				prunedNeighbors.add(entity);
			}
		}
		//for DS3 entities
		for (EntityProfileCompressed entity : allEntitiesFromDS3) {
			if (entity.getSimilarity() >= similarityAverageDS3) {
				prunedNeighbors.add(entity);
			}
		}
		
	
		out.collect(new Tuple2<String, Set<EntityProfileCompressed>>(current.getEntityUrl(), prunedNeighbors));
		
	}
	
	private void calculateDiverstityBetweenNeighbors(EntityProfileCompressed currentEntity, Map<EntityProfileCompressed, Set<Tuple2<Integer, Double>>> diversityBetweenNeighbors){
		if (diversityBetweenNeighbors.isEmpty()) {
			diversityBetweenNeighbors.put(currentEntity, new HashSet<>());
		} else {
			Set<Tuple2<Integer, Double>> set = new HashSet<Tuple2<Integer, Double>>();
			for (EntityProfileCompressed entityKey : diversityBetweenNeighbors.keySet()) {
				double diversity = computeDiversity(currentEntity.getTokensOfProperties(), entityKey.getTokensOfProperties());
				
				//add the entity tuple.f1.f1 in the set of entityKey
				diversityBetweenNeighbors.get(entityKey).add(new Tuple2<Integer, Double>(currentEntity.getKey(), diversity));
				
				//add in a new set with entityKey
				set.add(new Tuple2<Integer, Double>(entityKey.getKey(), diversity));
				
			}
			//create a new key tuple.f1.f1 with the current set
			diversityBetweenNeighbors.put(currentEntity, set);
		}
	}
	
	private double computeDiversity(Set<Integer> tokensPropertiesFromSource, Set<Integer> tokensPropertiesFromTarget) {
		Integer contentSize = tokensPropertiesFromTarget.size();
		
		tokensPropertiesFromTarget.removeAll(tokensPropertiesFromSource);
		return contentSize == 0 ? 0.0 : ((double)tokensPropertiesFromTarget.size())/contentSize;
	}

}
