package main;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import Data.EntityProfile;

public class CoGroupTokenBlocking implements
		CoGroupFunction<org.apache.flink.api.java.tuple.Tuple2<java.lang.Integer, java.util.List<Data.EntityProfile>>, org.apache.flink.api.java.tuple.Tuple2<java.lang.Integer, java.util.List<Data.EntityProfile>>, Tuple2<Integer, List<EntityProfile>>> {

	@Override
	public void coGroup(Iterable<Tuple2<Integer, List<EntityProfile>>> first,
			Iterable<Tuple2<Integer, List<EntityProfile>>> second, Collector<Tuple2<Integer, List<EntityProfile>>> out) throws Exception {
		if (second != null) {
			ArrayList<EntityProfile> neighbors = new ArrayList<EntityProfile>();
			for (Tuple2<Integer, List<EntityProfile>> tuplesTarget : second) {
				for (EntityProfile eTarget : tuplesTarget.f1) {
					if (eTarget != null) {
						neighbors.add(eTarget);
					}
				}
			}
			
			for (Tuple2<Integer, List<EntityProfile>> tuplesSource : first) {
				for (EntityProfile eSource : tuplesSource.f1) {
					eSource.addAllNeighbor(neighbors);
				}
				out.collect(tuplesSource);
			}
		}
		
		
	}

}
