package main;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import Data.EntityProfile;

public class EntityListReduce implements GroupReduceFunction<Tuple2<Integer, EntityProfile>, Tuple2<Integer, List<EntityProfile>>> {

	@Override
	public void reduce(Iterable<Tuple2<Integer, EntityProfile>> in, Collector<Tuple2<Integer, List<EntityProfile>>> out) {
		List<EntityProfile> entityList = new ArrayList<EntityProfile>();
		Integer key = null;

		for (Tuple2<Integer, EntityProfile> e : in) {
			if (key == null) {
				key = e.f0;
			}
			entityList.add(e.f1);
		}
		out.collect(new Tuple2<Integer, List<EntityProfile>>(key, entityList));

	}

}
