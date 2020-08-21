package main;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import Data.EntityProfile;
import Data.Property;

public class EntityReduce implements GroupReduceFunction<Tuple2<Integer, EntityProfile>, Tuple2<Integer, EntityProfile>> {

	@Override
	public void reduce(Iterable<Tuple2<Integer, EntityProfile>> in, Collector<Tuple2<Integer, EntityProfile>> out) {
		EntityProfile first = null;
		Integer key = null;

		for (Tuple2<Integer, EntityProfile> e : in) {
			if (first == null && key == null) {
				key = e.f0;
				first = e.f1;
			} else {
				first.addAllNeighbor(e.f1.getNeighbors());
			}
		}
		
		//comparing entities
		double similarityTotal = 0;
		for (EntityProfile neighbor : first.getNeighbors()) {
			double sim = computeSimilarity(first.generateTokens(), neighbor.generateTokens());//DÁ PRA MELHORAR EVCITAR COMPUTAR SEMPRE.
			neighbor.setSimilarity(sim);
			double div = computeDiversity(first.getProperties(), neighbor.getProperties());
			neighbor.setDiversity(div);
			similarityTotal += sim;
		}
		
		double similarityAverage = similarityTotal/first.getNeighbors().size();
		
		//performing WNP algorithm applying diversity criterion
		TreeSet<EntityProfile> prunedNeighbors = new TreeSet<EntityProfile>();
		for (EntityProfile entity : first.getNeighbors()) {
			if (entity.getSimilarity() >= similarityAverage) {
				prunedNeighbors.add(entity);
			}
		}
		
		first.setNeighbors(prunedNeighbors);
		
		out.collect(new Tuple2<Integer, EntityProfile>(key, first));
	}

	private double computeDiversity(Set<Property> propertiesFromSource, Set<Property> propertiesFromTarget) {
		Set<Integer> tokensFromSource = getTokensFromPropeties(propertiesFromSource);
		Set<Integer> tokensFromTarget = getTokensFromPropeties(propertiesFromTarget);
		Integer contentSize = tokensFromTarget.size();
		
		tokensFromTarget.removeAll(tokensFromSource);
		return contentSize == 0 ? 0.0 : ((double)tokensFromTarget.size())/contentSize;
	}

	private Set<Integer> getTokensFromPropeties(Set<Property> propertiesFromSource) {
		Set<Integer> output = new HashSet<Integer>();
		for (Property entity : propertiesFromSource) {
			output.addAll(((EntityProfile)entity.getValue()).generateTokens());
		}
		return output;
	}

	private double computeSimilarity(Set<Integer> source, Set<Integer> target) {
		Set<Integer> intersection = new HashSet<Integer>(source);
		intersection.retainAll(target);
		
		return ((double)intersection.size())/Math.max(source.size(), target.size());
	}
}
