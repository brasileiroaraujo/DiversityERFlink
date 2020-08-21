package main;

import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import Data.EntityProfile;

public class MetablockingStrategy implements FlatMapFunction<Tuple2<Integer,List<EntityProfile>>, Tuple2<Integer,EntityProfile>> {

	@Override
	public void flatMap(Tuple2<Integer, List<EntityProfile>> block,
			Collector<Tuple2<Integer, EntityProfile>> out) throws Exception {
		for (EntityProfile entity : block.f1) {
			out.collect(new Tuple2<Integer, EntityProfile>(entity.getKey(), entity));
		}
		
	}

	

}
