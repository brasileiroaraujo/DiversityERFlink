package compressed;

import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import Data.EntityProfileCompressed;

public class FilterBlocksCompressed implements FilterFunction<Tuple2<String, Set<EntityProfileCompressed>>> {

	@Override
	public boolean filter(Tuple2<String, Set<EntityProfileCompressed>> value) throws Exception {
		if (value.f1.isEmpty()) {
			return false;
		}
		return true;
	}

}
