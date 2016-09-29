package hadoop.utils;

import hadoop.entity.CommodityCountPair;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ReducerUtil {

    /**
     * Count each commodity in CommodityCountPair and set the sum value into a map.
     * @param iterator
     * @return
     */
    public static Map<String, Long> countCommodityIntoMap(
            Iterator<CommodityCountPair> iterator) {
        Map<String, Long> countMap = new HashMap<>();
        while (iterator.hasNext()) {
            CommodityCountPair commodityCountPair = iterator.next();
            String commodity = commodityCountPair.getCommodity().toString();
            long count = commodityCountPair.getCount().get();
            countMap.put(commodity, countMap.containsKey(commodity) ?
                    countMap.get(commodity) + count : count);
        }
        return countMap;
    }
}
