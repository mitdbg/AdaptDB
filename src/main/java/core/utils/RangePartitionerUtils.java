package core.utils;

/**
 * Created by ylu on 12/14/15.
 */
public class RangePartitionerUtils {
    public static int[] getIntCutPoints(String cutpoints){
        if(cutpoints.equals("NULL")){
            return null;
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
