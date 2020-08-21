package main;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import Data.EntityProfile;

public class FilterBlocks implements FilterFunction<Tuple2<Integer, EntityProfile>> {

	@Override
	public boolean filter(Tuple2<Integer, EntityProfile> value) throws Exception {
		if (!value.f1.getNeighbors().isEmpty()) {
			return true;
		}
		return false;
	}

}
