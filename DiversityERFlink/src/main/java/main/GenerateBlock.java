package main;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import Data.EntityProfile;

public class GenerateBlock implements JoinFunction<Tuple2<Integer, EntityProfile>, Tuple2<Integer, EntityProfile>, Tuple2<Integer, EntityProfile>> {
	@Override
	public Tuple2<Integer, EntityProfile> join(Tuple2<Integer, EntityProfile> e1, Tuple2<Integer, EntityProfile> e2) {
	e1.f1.addNeighbor(e2.f1);
	return e1;
	}
}
