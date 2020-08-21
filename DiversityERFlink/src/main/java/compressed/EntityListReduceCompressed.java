package compressed;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import Data.EntityProfile;
import Data.EntityProfileCompressed;

public class EntityListReduceCompressed implements GroupReduceFunction<Tuple2<Integer, EntityProfileCompressed>, Tuple2<Integer, List<EntityProfileCompressed>>> {

	@Override
	public void reduce(Iterable<Tuple2<Integer, EntityProfileCompressed>> in, Collector<Tuple2<Integer, List<EntityProfileCompressed>>> out) {
		List<EntityProfileCompressed> entityList = new ArrayList<EntityProfileCompressed>();
		Integer key = null;

		for (Tuple2<Integer, EntityProfileCompressed> e : in) {
			if (key == null) {
				key = e.f0;
			}
			entityList.add(e.f1);
		}
		out.collect(new Tuple2<Integer, List<EntityProfileCompressed>>(key, entityList));

	}

}
