package main;

import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import Data.EntityProfile;

public class ExtractToken implements FlatMapFunction<EntityProfile, Tuple2<Integer, EntityProfile>> {
	
	private int sourceID;

	public ExtractToken(int sourceID) {
		this.sourceID = sourceID;
	}

	@Override
	public void flatMap(EntityProfile e, Collector<Tuple2<Integer, EntityProfile>> out) throws Exception {
		Set<Integer> tokens = e.generateTokens();
		for (Integer key : tokens) {
			e.setSource(sourceID);
			out.collect(new Tuple2<Integer, EntityProfile>(key, e));
		}
		
	}

}
