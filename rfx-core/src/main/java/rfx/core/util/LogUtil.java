package rfx.core.util;

import java.io.File;
import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import rfx.core.configs.WorkerConfigs;
import rfx.core.util.CommonUtil.COLOR_CODE;

public class LogUtil {

	public static final String LOG_EXT = ".log";
	static String prefixLogFile = "info-log-";
	static String prefixErrorLogFile = "error-log-";
	
	public static String suffixLogFile = StringPool.BLANK;
	static boolean logFileHourly = true;
	static boolean debug = false;
	static String debugLogFolderPath = "";

	static AsynFileWriter logFileWriter = new AsynFileWriter(2000);
		
	public static void setDebug(boolean debug) {
		LogUtil.debug = debug;
	}
	
	public static boolean isDebug() {
		return debug;
	}

	public static void shutdownLogThreadPools() {
		logFileWriter.shutdownTimer();		
	}

	public static void setSuffixLogFile(String suffixLogFile) {
		LogUtil.suffixLogFile = "-" + suffixLogFile;
	}

	public static String getSuffixLogFile() {
		return suffixLogFile;
	}

	public static void setPrefixFileName(String topic) {
		prefixLogFile = topic + "-" + prefixLogFile;
		prefixErrorLogFile = topic + "-" + prefixErrorLogFile;
	}

	public static void setLogFileHourly(boolean logFileHourly) {
		LogUtil.logFileHourly = logFileHourly;
	}

	public static void i(Object tag, Object log) {
		i(tag, String.valueOf(log), false);
	}

	static Logger logger = Logger.getRootLogger();

	public static void i(Object tag, String log, boolean dumpToFile) {
		if (!(tag instanceof String)) {
			tag = tag.getClass().getName();
		}
		System.out.println(COLOR_CODE.ANSI_YELLOW + tag + COLOR_CODE.ANSI_RESET + " : " + log);
		if (dumpToFile) {
			dumpToFile(tag + " : " + log, false, new Date());
		}
	}
	
	public static void logToFile(String log) {
		dumpToFile(log, false, new Date());		
	}


	public static void info(Object objectClass, String functionName, String log, boolean dumpToFile) {
		if (!(objectClass instanceof String)) {
			objectClass = objectClass.getClass().getName();
		}
		System.out.println(COLOR_CODE.ANSI_YELLOW + objectClass + "." + functionName + COLOR_CODE.ANSI_RESET + " : "+ log);
		Date currentDate = new Date();
		if (dumpToFile) {
			String s = String.format("%s.%s:%s", objectClass , functionName , log);
			dumpToFile(s, false, currentDate);
		}
	}

	public static void error(Object objectClass, String functionName, String log) {
		System.err.println(COLOR_CODE.ANSI_RED + objectClass + "." + functionName + COLOR_CODE.ANSI_RESET + " : " + log);
		Date currentDate = new Date();
		dumpToFile(objectClass + "." + functionName + " : " + log, true, currentDate);
	}

	public static void r(Object tag, String log) {

	}

	public static void d(Object tag, String log) {
		d(tag, log, false);
	}

	public static void d(String log) {
		d(StringPool.BLANK, log, false);
	}

	public static void d(Object tag, String log, boolean dumpToFile) {
		if (debug) {
			String s = COLOR_CODE.ANSI_GREEN + tag + COLOR_CODE.ANSI_RESET + " : " + log;
			System.out.println(s);
			if (dumpToFile) {
				dumpToFile(log,false,new Date());
			}
		}
	}

	
	static String getDebugFolderPath(){
		if(debugLogFolderPath.isEmpty()){
			WorkerConfigs workerConfigs = WorkerConfigs.load();
			debugLogFolderPath = workerConfigs.getDebugLogPath();
		}
		return debugLogFolderPath;
	}
	
	private static void dumpToFile(final String logData, final boolean isErrorMode, final Date loggedDate) {
		String debugLogFolderPath = getDebugFolderPath();
		if (!StringUtil.isEmpty(debugLogFolderPath)) {
			String dateFolder = DateTimeUtil.formatDate(loggedDate, "yyyy-MM-dd");
			String debugLogByDatePath = StringUtil.toString(debugLogFolderPath , "/" , dateFolder, "/");
			File dirPath = new File(debugLogByDatePath);
			if (!dirPath.isDirectory()) {
				dirPath.mkdir();
			}
			String prefixMode = isErrorMode ? prefixErrorLogFile : prefixLogFile;
			String datetime;
			if (logFileHourly) {
				datetime = DateTimeUtil.formatDate(loggedDate, "yyyy-MM-dd-HH");
			} else {
				datetime = DateTimeUtil.formatDate(loggedDate, "yyyy-MM-dd");
			}
			String time = "[" + DateTimeUtil.formatDate(loggedDate, "yyyy-MM-dd HH:mm:ss") + "] ";
			String data = time + logData + "\n";
			String path =  StringUtil.toString(debugLogByDatePath, prefixMode , datetime , suffixLogFile ,LOG_EXT);
			//System.out.println(path);
			logFileWriter.write(path, data);
		}

	}

	public static void i(String log) {
		System.out.println(COLOR_CODE.ANSI_YELLOW + log + COLOR_CODE.ANSI_RESET);
	}

	public static void e(Object tag, Object log) {
		System.err.println(COLOR_CODE.ANSI_RED + tag + COLOR_CODE.ANSI_RESET + " : " + log);
		dumpToFile(StringUtil.toString(tag,":",log),true,new Date());
	}

	public static void error(Object log) {
		System.err.println(log);		
		dumpToFile(String.valueOf(log),true,new Date());
	}

	public static void error(Exception e) {
		dumpToFile(ExceptionUtils.getStackTrace(e), true, new Date());
	}
}
