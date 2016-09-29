package hadoop.mapreduce;

import hadoop.entity.CommodityCountPair;
import hadoop.utils.ReducerUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Summary the count of common item at each mapper before reduce.
 * Reducer will get less input records.
 */
public class RecommendCombiner extends MapReduceBase
        implements Reducer<Text, CommodityCountPair, Text, CommodityCountPair> {

    /**
     * Input: <cd12, (book34, 1)>, <cd12, (dvd32, 1)>, <book34, (cd54, 1)>, ...
     * Output: <cd12, (book34, 3)>, <cd12, (dvd32, 1)>, <book34, (cd54, 5)>, ...
     * @param text
     * @param iterator
     * @param outputCollector
     * @param reporter
     * @throws IOException
     */
    @Override
    public void reduce(Text text, Iterator<CommodityCountPair> iterator,
                       OutputCollector<Text, CommodityCountPair> outputCollector,
                       Reporter reporter) throws IOException {
        Map<String, Long> countMap = ReducerUtil.countCommodityIntoMap(iterator);

        Iterator iter = countMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String commodity = (String) (entry.getKey());
            long count = (Long) (entry.getValue());
            outputCollector.collect(text, new CommodityCountPair(commodity, count));
        }
    }
}
