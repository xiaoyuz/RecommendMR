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

public class RecommendReducer extends MapReduceBase
        implements Reducer<Text, CommodityCountPair, Text, Text>{

    // CAN BE MODIFIED.
    // Commodities whose count value is not small than THRESHOLD_VALUE
    // will be regard as commonly bought one.
    // Here 3 for test and debug.
    private static final long THRESHOLD_VALUE = 3;

    /**
     * Input: <cd12, (book34, 3)>, <cd12, (dvd32, 1)>, <book34, (cd54, 5)>, ...
     * Output: cd12    book34, book12, cd21, dvd11....
     * @param text
     * @param iterator
     * @param outputCollector
     * @param reporter
     * @throws IOException
     */
    @Override
    public void reduce(Text text, Iterator<CommodityCountPair> iterator,
                       OutputCollector<Text, Text> outputCollector,
                       Reporter reporter) throws IOException {
        StringBuffer result = new StringBuffer();
        Map<String, Long> countMap = ReducerUtil.countCommodityIntoMap(iterator);

        Iterator countMapIter = countMap.entrySet().iterator();
        while (countMapIter.hasNext()) {
            Map.Entry entry = (Map.Entry) countMapIter.next();
            if (((Long) entry.getValue()) >= THRESHOLD_VALUE) {
                result.append(entry.getKey().toString()).append(", ");
            }
        }
        outputCollector.collect(text, new Text(result.toString()));
    }
}
