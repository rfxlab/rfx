package rfx.core.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AntBuilderUtil {
	
	static List<File> files = new ArrayList<>();
	
	public static void walk(String path ) {

        File root = new File( path );
        File[] list = root.listFiles();

        if (list == null) return;

        for ( File f : list ) {
            if ( f.isDirectory() ) {
                walk( f.getAbsolutePath() );
                //System.out.println( "Dir:" + f.getAbsoluteFile() );
            }
            else {
                //System.out.println( "File:" + f.getAbsoluteFile() );
                files.add(f);
            }
        }
    }
	
	 public static void main(String[] args) 
	 {
	  
	   // Directory path here
	   String path = "/home/trieunt/projects/vertx-hacking/stream-processor/lib"; 
	  
	   File folder = new File(path);
	   String fullPathLibs = folder.getAbsolutePath();
	   
	   walk(fullPathLibs);
	   
	   Collections.sort(files);
	   for (int i = 0; i < files.size(); i++) 
	   {
		   String libPath = files.get(i).getAbsolutePath();
		   if(libPath.endsWith(".jar")){
			   String name = libPath.replace(fullPathLibs, "");			   
			   StringBuilder s = new StringBuilder(path);
			   s.append(name).append(" ; ");
			   System.out.println(s);
		   }	           
	   }
	 }
}
