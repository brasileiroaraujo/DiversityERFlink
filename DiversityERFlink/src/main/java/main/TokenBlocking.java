package main;

import java.util.List;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import Data.EntityProfile;

public class TokenBlocking implements JoinFunction<Tuple2<Integer, List<EntityProfile>>, Tuple2<Integer, List<EntityProfile>>, Tuple2<Integer, List<EntityProfile>>> {

	@Override
	public Tuple2<Integer, List<EntityProfile>> join(Tuple2<Integer, List<EntityProfile>> first,
			Tuple2<Integer, List<EntityProfile>> second) throws Exception {
		if (second != null) {
			for (EntityProfile eSource : first.f1) {
				eSource.addAllNeighbor(second.f1);
			}
		}
		return first;
	}
}
