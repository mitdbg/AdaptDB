package core.utils;

import java.io.File;

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
	
	
	public static void main(String[] args){
		File f = new File("test/lineitem.tbl");
		System.out.print(String.format("file exits: %s smaller than free mem %s", f.exists(), fileLargerThanFree(f)));
	}
}
