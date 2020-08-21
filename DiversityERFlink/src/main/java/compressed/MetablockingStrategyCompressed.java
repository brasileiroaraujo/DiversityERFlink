package compressed;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import Data.EntityProfileCompressed;
import Data.Property;

public class MetablockingStrategyCompressed implements FlatMapFunction<Tuple2<Integer,List<EntityProfileCompressed>>, Tuple2<Integer, Tuple2<EntityProfileCompressed, EntityProfileCompressed>>> {

	@Override
	public void flatMap(Tuple2<Integer, List<EntityProfileCompressed>> block,
			Collector<Tuple2<Integer, Tuple2<EntityProfileCompressed, EntityProfileCompressed>>> out) throws Exception {
		List<EntityProfileCompressed> entityFromSource = new ArrayList<EntityProfileCompressed>();
		
		//the entities from DS1 are the first, after compare with others 
		for (EntityProfileCompressed entity : block.f1) {
			if (entity.getSourceID() == 1) {
				entityFromSource.add(entity);
			} else {
				for (EntityProfileCompressed entitySource : entityFromSource) {
					entity.setSimilarity(computeSimilarity(entitySource.getTokens(), entity.getTokens()));
					entity.setDiversity(computeDiversity(entitySource.getTokensOfProperties(), entity.getTokensOfProperties()));
					out.collect(new Tuple2<Integer, Tuple2<EntityProfileCompressed, EntityProfileCompressed>>(entitySource.getKey(), new Tuple2<EntityProfileCompressed, EntityProfileCompressed>(entitySource, entity)));
				}
			}
		}
		
	}
	
	private double computeDiversity(Set<Integer> tokensPropertiesFromSource, Set<Integer> tokensPropertiesFromTarget) {
		Integer contentSize = tokensPropertiesFromTarget.size();
		
		tokensPropertiesFromTarget.removeAll(tokensPropertiesFromSource);
		return contentSize == 0 ? 0.0 : ((double)tokensPropertiesFromTarget.size())/contentSize;
	}
	
	private double computeSimilarity(Set<Integer> source, Set<Integer> target) {
		Set<Integer> intersection = new HashSet<Integer>(source);
		intersection.retainAll(target);
		
		return ((double)intersection.size())/Math.min(source.size(), target.size());
	}

	

}
