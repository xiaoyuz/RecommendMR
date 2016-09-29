package hadoop;

import hadoop.entity.CommodityCountPair;
import hadoop.mapreduce.RecommendCombiner;
import hadoop.mapreduce.RecommendMapper;
import hadoop.mapreduce.RecommendReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class RecommendMain {

    private static final String JOB_NAME = "recommend MR";

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(RecommendMain.class);
        conf.setJobName(JOB_NAME);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(CommodityCountPair.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(RecommendMapper.class);
        conf.setCombinerClass(RecommendCombiner.class);
        conf.setReducerClass(RecommendReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

}
