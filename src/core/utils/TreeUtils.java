package core.utils;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

public class TreeUtils {

	public TreeUtils() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * Coarse estimate 
	 * @param file
	 * @return
	 */
	public static boolean fileLargerThanFree(File file){
		if (file.exists()){
			Runtime runtime = Runtime.getRuntime();
			if (file.length() > runtime.freeMemory()){
				return false;
			}
			
		}
		return true;
	}
	
    public static void plot(Map<String, Integer> buckets, String chartFileName) throws IOException{
        final String cnt = "COUNT";


        final DefaultCategoryDataset dataset = new DefaultCategoryDataset( );

        for(Entry<String, Integer> kv : buckets.entrySet()){
        	dataset.addValue( kv.getValue() , cnt , kv.getKey());
        }


        JFreeChart barChart = ChartFactory.createBarChart(
           "Bucket Counts", 
           "Bucket", "Count", 
           dataset,PlotOrientation.VERTICAL, 
           true, true, false);
           
        int width = 640; /* Width of the image */
        int height = 480; /* Height of the image */ 
        File BarChart = new File(chartFileName); 
        ChartUtilities.saveChartAsJPEG( BarChart , barChart , width , height );
    	
    }
	
	
	public static void main(String[] args){
		File f = new File("test/lineitem.tbl");
		System.out.print(String.format("file exits: %s smaller than free mem %s", f.exists(), fileLargerThanFree(f)));
	}
}
