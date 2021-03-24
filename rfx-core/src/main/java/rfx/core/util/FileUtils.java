package rfx.core.util;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import javax.activation.MimetypesFileTypeMap;

import org.apache.commons.net.util.Base64;

/**
 * Work with files in file system (delete, get size, etc...).
 * 
 * @author
 * 
 */
public class FileUtils {
	static String baseFolderPath = "";

	public static String getRuntimeFolderPath()  {
		if(baseFolderPath.isEmpty()){
			try {
				File dir1 = new File(".");
				baseFolderPath = dir1.getCanonicalPath();
			} catch (IOException e) {			
				e.printStackTrace();
			}
		}		
		return baseFolderPath;
	}

	
	public static String readFileAsString(String uri, String defaultStr){
		try {
			return readFileAsString(uri);
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
		return defaultStr;
	}
	
	public static String readFileAsString(String uri) throws java.io.IOException {			
		StringBuffer fileData = new StringBuffer(1000);
		String fullpath = FileUtils.getRuntimeFolderPath() + uri.replace("/", File.separator);
		if(!uri.startsWith("/")){
			fullpath = FileUtils.getRuntimeFolderPath() + File.separator + uri.replace("/", File.separator);
		}
		
		//System.out.println(fullpath);
		BufferedReader reader = new BufferedReader(new FileReader(fullpath));
		char[] buf = new char[2048];
		int numRead = 0;
		while ((numRead = reader.read(buf)) != -1) {
			fileData.append(buf, 0, numRead);
		}
		reader.close();
		return fileData.toString();
	}
	
	public static String readAbsolutePathFileAsString(String fullpath) throws java.io.IOException {			
		StringBuffer fileData = new StringBuffer(1000);
		BufferedReader reader = new BufferedReader(new FileReader(fullpath));
		char[] buf = new char[2048];
		int numRead = 0;
		while ((numRead = reader.read(buf)) != -1) {
			fileData.append(buf, 0, numRead);
		}
		reader.close();
		return fileData.toString();
	}
		
	
	public static File[]  listFilesInForder(String folderPath) throws java.io.IOException {
		String fullpath = FileUtils.getRuntimeFolderPath() + folderPath;		
		File folder = new File(fullpath);
		if(folder.isDirectory())
			return folder.listFiles();
		return new File[0];
	}
	
	public static File[]  listFilesInForder(String folderPath, FilenameFilter filenameFilter) throws java.io.IOException {
		String fullpath = FileUtils.getRuntimeFolderPath() + folderPath;		
		File folder = new File(fullpath);
		if(folder.isDirectory())
			return folder.listFiles(filenameFilter);
		return new File[0];
	}
	
	public static DataInputStream readFileAsStream(String filePath)  {
		try {
			String fullpath = FileUtils.getRuntimeFolderPath() + filePath;		
			DataInputStream  stream = new DataInputStream(new FileInputStream(fullpath));		
			return stream;
		} catch (IOException e) {
			
			if(e instanceof java.io.FileNotFoundException){
				System.err.println("The system cannot find the file " + filePath );
			} else {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	public static String loadFilePathToString(String fullPath) throws java.io.IOException {		
		StringBuffer fileData = new StringBuffer(1000);
		BufferedReader reader = new BufferedReader(new FileReader(fullPath));
		char[] buf = new char[2048];
		int numRead = 0;
		while ((numRead = reader.read(buf)) != -1) {
			fileData.append(buf, 0, numRead);
		}
		reader.close();
		return fileData.toString();
	}

	/**
	 * Closes InputStream and/or OutputStream. It makes sure that both streams
	 * tried to be closed, even first throws an exception.
	 * 
	 * @throw IOException if stream (is not null and) cannot be closed.
	 * 
	 */
	protected static void close(InputStream iStream, OutputStream oStream)
			throws IOException {
		try {
			if (iStream != null) {
				iStream.close();
			}
		} finally {
			if (oStream != null) {
				oStream.close();
			}
		}
	}
	
	public static boolean writeStringToFile(String fullPath, String content){		
		try {
			Writer output = null;
			File file = new File(fullPath);
			if (!file.exists()) {
				file.createNewFile();
			}
			output = new BufferedWriter(new FileWriter(file));
			output.write(content);
			output.close();
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
		return true;		
	}
	
	public static boolean writeStringToFile(String fullPath, byte[] fileBytes){
		try {
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fullPath));
			bos.write(fileBytes);
			bos.flush();
			bos.close();
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
		return true;
	}
	
	public static List<String> listAllFilesInRunntimeDir(final String ext){
		final List<String> list = new ArrayList<String>();
		File dir1 = new File(".");
		File[] files = dir1.listFiles(new FileFilter() {			
			@Override
			public boolean accept(File pathname) {
				if(! ext.isEmpty()){
					if(pathname.getName().endsWith(ext) && pathname.isFile()){						
						return true;
					} else {
						return false;
					}
				} 				
				return true;
			}
		});
		for (File file : files) {			
			list.add(file.getAbsolutePath());
		}
		return list;
	}
	
	
	public static String encodeFileToBase64Binary(String fileName)
			throws IOException { 
		File file = new File(fileName);
		byte[] bytes = loadFile(file);
		byte[] encoded = Base64.encodeBase64(bytes);
		String encodedString = new String(encoded); 
		return encodedString;
	}
	
	public static String getContentType(String filename){
		File f = new File("./resources" + filename);
		return new MimetypesFileTypeMap().getContentType(f);		
	}
 
	public static byte[] loadFile(File file) throws IOException {
	    InputStream is = new FileInputStream(file);
 
	    long length = file.length();
	    if (length > Integer.MAX_VALUE) {
	        // File is too large
	    }
	    byte[] bytes = new byte[(int)length];
	    
	    int offset = 0;
	    int numRead = 0;
	    while (offset < bytes.length
	           && (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
	        offset += numRead;
	    }
 
	    if (offset < bytes.length) {
	        throw new IOException("Could not completely read file "+file.getName());
	    }
 
	    is.close();
	    return bytes;
	}
}
