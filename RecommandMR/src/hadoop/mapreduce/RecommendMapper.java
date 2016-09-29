package hadoop.mapreduce;

import hadoop.entity.CommodityCountPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;

public class RecommendMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, CommodityCountPair> {

    /**
     * Input: cd12, book34, dvd32, book11, dvd32, cd54, book32
     * Output: <cd12, (book34, 1)>, <cd12, (dvd32, 1)>, <book34, (cd54, 1)>, ...
     * @param longWritable
     * @param text
     * @param outputCollector
     * @param reporter
     * @throws IOException
     */
    @Override
    public void map(LongWritable longWritable, Text text,
                    OutputCollector<Text, CommodityCountPair> outputCollector,
                    Reporter reporter) throws IOException {
        LinkedHashSet<String> commoditiesSet = new LinkedHashSet<>();
        StringTokenizer tokenizer = new StringTokenizer(text.toString(), ",");
        while (tokenizer.hasMoreTokens()) {
            commoditiesSet.add(tokenizer.nextToken().trim());
        }
        List<String> commoditiesList = new ArrayList<>(commoditiesSet);
        // Record every pair of commodity which be bought at the same time.
        // Each record has a count value, for the sake of summary counting in reducer
        // and choose the most commonly bought ones via the count value.
        // Maybe it is a large data set.
        for (int i = 0; i < commoditiesList.size(); i++) {
            for (int j = 0; j < commoditiesList.size(); j++) {
                if (i != j) {
                    outputCollector.collect(new Text(commoditiesList.get(i)),
                            new CommodityCountPair(commoditiesList.get(j), 1));
                }
            }
        }
    }
}
