package core.utils;

import org.apache.hadoop.io.LongWritable;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ylu on 12/14/15.
 */
public class RangePartitionerUtils {
    public static int[] getCutPoints(List<LongWritable> sampleKeys, int n){
        long[] keys = new long[sampleKeys.size()];
        int i = 0;
        for(LongWritable lw : sampleKeys){
            keys[i++] = lw.get();
        }
        Arrays.sort(keys);

        int m = keys.length;
        int[] cutpoints = new int[n - 1];

        int small = n - m % n;
        int large = m % n;

        int pi = 0, pj = 0;
        for(int j = 0; j < small; j ++){
            pj += m / n;
            if(pi < cutpoints.length){
                cutpoints[pi ++ ] = (int)keys[pj];
            }

        }
        for(int j = 0; j < large; j ++){
            pj += m / n + 1;
            if(pi < cutpoints.length){
                cutpoints[pi ++ ] = (int)keys[pj];
            }
        }
        return cutpoints;
    }

    public static int[] getIntCutPoints(String cutpoints){
        if(cutpoints.equals("NULL")){
            return null;
        }
        if(cutpoints.length() == 0){
            return new int[0];
        }
        String[] values = cutpoints.split(",");
        int[] ranges = new int[values.length];
        for(int i = 0 ; i < ranges.length; i ++){
            ranges[i] = Integer.parseInt(values[i]);
        }
        return ranges;
    }
    public static String getStringCutPoints(int[] cutpoints){
        StringBuilder sb  = new StringBuilder();
        for(int i = 0 ;i < cutpoints.length; i ++){
            if(sb.length() > 0){
                sb.append(",");
            }
            sb.append(cutpoints[i]);
        }
        return sb.toString();
    }
    public static int[] getSplits(int[] cutpoints){
        int[] splits = new int[cutpoints.length + 1];
        for(int i = 0 ;i < splits.length; i ++){
            splits[i] = i;
        }
        return splits;
    }
}
