package compressed;

import java.util.List;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import Data.EntityProfile;
import Data.EntityProfileCompressed;

public class TokenBlockingCompressed implements JoinFunction<Tuple2<Integer, List<EntityProfileCompressed>>, Tuple2<Integer, List<EntityProfileCompressed>>, Tuple2<Integer, List<EntityProfileCompressed>>> {

	@Override
	public Tuple2<Integer, List<EntityProfileCompressed>> join(Tuple2<Integer, List<EntityProfileCompressed>> first,
			Tuple2<Integer, List<EntityProfileCompressed>> second) throws Exception {
		if (second != null) {
			first.f1.addAll(second.f1);
		}
		return first;
	}
}
