package hadoop.entity;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Entity of common item with the count that other customers also bought it at the same time.
 */
public class CommodityCountPair implements WritableComparable<CommodityCountPair> {

    private Text mCommodity;
    private LongWritable mCount;

    public CommodityCountPair() {
        set(new Text(), new LongWritable());
    }

    public CommodityCountPair(String commodity, long count) {
        set(new Text(commodity), new LongWritable(count));
    }

    public CommodityCountPair(Text commodity, LongWritable count) {
        set(commodity, count);
    }

    public void set(Text commodity, LongWritable count) {
        this.mCommodity = commodity;
        mCount = count;

    }

    public Text getCommodity() {
        return mCommodity;
    }

    public LongWritable getCount() {
        return mCount;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        mCommodity.readFields(in);
        mCount.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        mCommodity.write(out);
        mCount.write(out);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CommodityCountPair) {
            CommodityCountPair cp = (CommodityCountPair) o;
            return mCommodity.equals(cp.getCommodity())
                    && mCount.equals(cp.getCount());
        }
        return false;
    }

    @Override
    public String toString() {
        return mCommodity + ":" + mCount;
    }

    @Override
    public int compareTo(CommodityCountPair cp) {
        int cmp = mCommodity.compareTo(cp.getCommodity());
        if (cmp != 0)
            return cmp;
        return mCount.compareTo(cp.getCount());
    }
}
