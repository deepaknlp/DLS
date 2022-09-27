//--------------------------------------------------------------------------------------------------------
// FormatUtils.java
//--------------------------------------------------------------------------------------------------------

package hiD.utils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

// import bedrock.log.*;

//--------------------------------------------------------------------------------------------------------
// FormatUtils
//--------------------------------------------------------------------------------------------------------

public class FormatUtils implements Constants {

//--------------------------------------------------------------------------------------------------------
// FormatUtils consts
//--------------------------------------------------------------------------------------------------------

  private static final Runtime            kRuntime=Runtime.getRuntime();
  private static final SimpleDateFormat   kStdTimeFormat=new SimpleDateFormat("yyyy:MM:dd HH:mm:ss.SSS");

  public static final String              kDivider=
      "//--------------------------------------------------------------------------------------------------------";
  
//--------------------------------------------------------------------------------------------------------
// Logging bottleneck
//
// Can redirect all log msgs to some other logging system by changing the following
//--------------------------------------------------------------------------------------------------------
  
  public static final void log(String inString) {
//    if (kOnDevBox)
//      Log.logInfo("HiD",inString);
//    else {
      String theString=inString;
      int thePos=theString.indexOf('\n');
      while (thePos>kNotFound) {
        System.out.println(
            rightPad(formatTime(System.currentTimeMillis()),22)+
            leftPad(formatMemory(kRuntime.totalMemory()-kRuntime.freeMemory()),10)+
            "  "+theString.substring(0,thePos));
        theString=theString.substring(thePos+1);
        thePos=theString.indexOf('\n');
      }
      
      System.out.println(
          rightPad(formatTime(System.currentTimeMillis()),22)+
          leftPad(formatMemory(kRuntime.totalMemory()-kRuntime.freeMemory()),10)+
          "  "+theString);
//    }
  }

  public static final void log(String inString, Throwable t) {
    log(formatException(inString,t)); }

//--------------------------------------------------------------------------------------------------------
// leftPad
//--------------------------------------------------------------------------------------------------------

  public static String leftPad(String inString, int inWidth) {
    int theLength=(inString==null?0:inString.length());
    int theWidth=Math.max(theLength,inWidth);
    StringBuffer theBuffer=new StringBuffer(theWidth);
    for (int i=theLength; i<theWidth; i++)
      theBuffer.append(' ');
    if (inString!=null)
      theBuffer.append(inString);
    return theBuffer.toString();
  }

  public static String leftPad(long inNumber, int inWidth) {
    return leftPad(String.valueOf(inNumber),inWidth); }

  public static String leftPad(double inNumber, int inNDecimals, int inWidth) {
    return leftPad(formatDouble(inNumber,inNDecimals),inWidth); }

  public static String leftPad(double inNumber, int inWidth) {
    return leftPad(formatDouble(inNumber),inWidth); }

//--------------------------------------------------------------------------------------------------------
// rightPad
//--------------------------------------------------------------------------------------------------------

  public static String rightPad(String inString, int inWidth) {
    int theLength=(inString==null?0:inString.length());
    int theWidth=Math.max(theLength,inWidth);
    StringBuffer theBuffer=new StringBuffer(theWidth);
    if (inString!=null)
      theBuffer.append(inString);
    for (int i=theLength; i<theWidth; i++)
      theBuffer.append(' ');
    return theBuffer.toString();
  }

  public static String rightPad(long inNumber, int inWidth) {
    return rightPad(String.valueOf(inNumber),inWidth); }

  public static String rightPad(double inNumber, int inNDecimals, int inWidth) {
    return rightPad(formatDouble(inNumber,inNDecimals),inWidth); }

  public static String rightPad(double inNumber, int inWidth) {
    return rightPad(formatDouble(inNumber),inWidth); }
  
//--------------------------------------------------------------------------------------------------------
// formatDouble
//--------------------------------------------------------------------------------------------------------

  public static String formatDouble(double inDouble, int inNDecimals) {
    return String.format("%."+inNDecimals+"f",inDouble); }

  public static String formatDouble(double inDouble) {
    return formatDouble(inDouble,3); }
  
//--------------------------------------------------------------------------------------------------------
// formatPercent
//--------------------------------------------------------------------------------------------------------

  public static String formatPercent(double inFraction, int inNDecimals) {
    return formatDouble(inFraction*100.0,inNDecimals)+"%"; }

  public static String formatPercent(double inFraction) { 
    return formatPercent(inFraction,3); }
  
//--------------------------------------------------------------------------------------------------------
// formatDistance2
//--------------------------------------------------------------------------------------------------------

  public static String formatDistance2(double inDistance2) {
    return ((inDistance2>1e12)?"infinity":formatDouble(inDistance2)); }
  
//--------------------------------------------------------------------------------------------------------
// formatTime
//--------------------------------------------------------------------------------------------------------

  public static String formatTime(long inTime, SimpleDateFormat inDateFormat) {
    synchronized(inDateFormat) { return inDateFormat.format(new Date(inTime)); }}

  public static String formatTime(long inTime) {
    return formatTime(inTime,kStdTimeFormat); }
  
//--------------------------------------------------------------------------------------------------------
// formatDuration
//--------------------------------------------------------------------------------------------------------

  public static String formatDuration(double inDuration) {    
    if (inDuration==kNotFound)
      return "";
    
    // Maintain 4 sig figs throughout
    String theTimeStr=null;
    double theDuration=Math.abs(inDuration);
    if (theDuration<0.00099995) {
      // 1.000 - 999.9 ns
      theDuration*=1000000.0;
      if (theDuration<9.9995)
        theTimeStr=formatDouble(theDuration)+"ns";
      else if (theDuration<99.995)
        theTimeStr=formatDouble(theDuration,2)+"ns";
      else
        theTimeStr=formatDouble(theDuration,1)+"ns";
    } else if (theDuration<0.9995) {
      // 1.000 - 999.9 µs
      theDuration*=1000.0;
      if (theDuration<9.9995)
        theTimeStr=formatDouble(theDuration)+"µs";
      else if (theDuration<99.995)
        theTimeStr=formatDouble(theDuration,2)+"µs";
      else
        theTimeStr=formatDouble(theDuration,1)+"µs";
    } else if (theDuration<9999.5) {
      // 1.000 - 999.9 ms
      if (theDuration<9.9995)
        theTimeStr=formatDouble(theDuration)+"ms";
      else if (theDuration<99.995)
        theTimeStr=formatDouble(theDuration,2)+"ms";
      else
        theTimeStr=formatDouble(theDuration,1)+"ms";
    } else {
      // 1.000 - 99.99 secs
      theDuration/=1000.0;
      if (theDuration<9.9995)
        theTimeStr=formatDouble(theDuration)+"secs";
      else if (theDuration<99.995)
        theTimeStr=formatDouble(theDuration,2)+"secs";
      else {
        // 1.667 - 99.99 mins
        theDuration/=60.0;
        if (theDuration<9.9995)
          theTimeStr=formatDouble(theDuration)+"mins";
        else if (theDuration<99.995)
          theTimeStr=formatDouble(theDuration,2)+"mins";
        else {
          // 1.667 - 99.99 hrs 
          theDuration/=60.0;
          if (theDuration<9.9995)
            theTimeStr=formatDouble(theDuration)+"hrs";
          else if (theDuration<99.995)
            theTimeStr=formatDouble(theDuration,2)+"hrs";
          // 4.167 + days
          else {
            theDuration/=24.0;
            if (theDuration<9.9995)
              theTimeStr=formatDouble(theDuration)+"days";
            else if (theDuration<99.995)
              theTimeStr=formatDouble(theDuration,2)+"days";
            else if (theDuration<999.95)
              theTimeStr=formatDouble(theDuration,1)+"days";
            else 
              theTimeStr=Math.round(theDuration)+"days";
          }
        }
      }
    }
    if (inDuration<0.0)
      theTimeStr="-"+theTimeStr;
    return theTimeStr;
  }
  
//--------------------------------------------------------------------------------------------------------
// formatMemory
//--------------------------------------------------------------------------------------------------------

  public static String formatMemory(long inMemory) {
    if (inMemory==kNotFound)
      return "";
    String theMemoryStr=null;
    long theMemory=Math.abs(inMemory);
    if (theMemory<5000)
      theMemoryStr=theMemory+"B ";
    else {
      double theRealMemory=theMemory/1024.0;
      if (theRealMemory<999.995)
        theMemoryStr=formatDouble(theRealMemory)+"KB";
      else {
        theRealMemory/=1024.0;
        if (theRealMemory<999.995)
          theMemoryStr=formatDouble(theRealMemory)+"MB";
        else {
          theRealMemory/=1024.0;
          if (theRealMemory<999.995)
            theMemoryStr=formatDouble(theRealMemory)+"GB";
          else {
            theRealMemory/=1024.0;
            if (theRealMemory<999.995)
              theMemoryStr=formatDouble(theRealMemory)+"TB";
            else {
              theRealMemory/=1024.0;
              theMemoryStr=formatDouble(theRealMemory)+"PB";
            }
          }
        }
      }
    }
    if (inMemory>=0)
      return theMemoryStr;
    else
      return '-'+theMemoryStr;
  }

//--------------------------------------------------------------------------------------------------------
// formatException
//--------------------------------------------------------------------------------------------------------

  public static String formatException(String inMessage, Throwable inThrowable) {
    StringWriter theBuffer=new StringWriter();
    PrintWriter theWriter=new PrintWriter(theBuffer);
    if (inMessage!=null)
      theWriter.println(inMessage);
    inThrowable.printStackTrace(theWriter);
    return theBuffer.toString();
  }

//--------------------------------------------------------------------------------------------------------
// standard filename structure
//   datasets look like this:    OpenI_E_512D_667Kv.vecs
//   indexes look like this:     OpenI_E_512D_667Kv_50Nr.ndx
// where
//   OpenI_E is the source name
//   512D is the number of dimensions
//   667Kv is the number of vectors   (Kv, Mv, and GV supported)
//   50Nr is the number of nearest neighbors included
//   *.vecs indicates vector file
//   *.ndx indicates index file
//--------------------------------------------------------------------------------------------------------

  public static String stripFilePath(String inFilename) {
    int thePos=Math.max(inFilename.lastIndexOf('/'),inFilename.lastIndexOf('\\'));
    return (thePos<0)?inFilename:inFilename.substring(thePos+1);
  }

  public static String stripFileType(String inFilename) {
    int thePos=inFilename.lastIndexOf(".");
    return (thePos<0)?inFilename:inFilename.substring(0,thePos);
  }

  public static String stripFilePathAndType(String inFilename) {
    return stripFileType(stripFilePath(inFilename));
  }

  public static String standardDataSetFilename(String inSourceName, int inNDims, int inNVectors) {
    String theFilenamePrefix=inSourceName+"_"+inNDims+"D_";
    if (inNVectors<10000)
      return theFilenamePrefix+inNVectors+"v.vecs";
    else if (inNVectors<10000000)
      return theFilenamePrefix+(inNVectors/1000)+"Kv.vecs";
    else if (inNVectors<10000000000L)
      return theFilenamePrefix+(inNVectors/1000000)+"Mv.vecs";
    else
      return theFilenamePrefix+(inNVectors/1000000000)+"Gv.vecs";
  }

  public static String standardIndexFilename(String inDataSetFilename, int inIndexNNear) {
    return stripFilePath(
        inDataSetFilename.substring(0,inDataSetFilename.length()-5)+"_"+inIndexNNear+"Nr.ndx"); }

  // Works with source, dataset, and index filenames
  public static String extractSourceName(String inFilename) {
    String theSourceName=stripFilePathAndType(inFilename);
    int theNParams=0;
    if (inFilename.endsWith(".ndx"))
      theNParams=3;
    if (inFilename.endsWith(".vecs"))
      theNParams=2;
    for (int i=0; i<theNParams; i++)
      theSourceName=theSourceName.substring(0,theSourceName.lastIndexOf("_"));
    return theSourceName;
  }

  public static String extractIndexDataSetFilename(String inIndexFilename) {
    String theIndexFilename=stripFilePathAndType(inIndexFilename);
    return theIndexFilename.substring(0,theIndexFilename.lastIndexOf("_"))+".vecs";
  }

  public static int extractIndexNNear(String inIndexFilename) {
    String theCore=stripFilePathAndType(inIndexFilename);
    return Integer.parseInt(theCore.substring(theCore.lastIndexOf("_")+1,theCore.length()-2));
  }

//--------------------------------------------------------------------------------------------------------
// reportBanner
//--------------------------------------------------------------------------------------------------------

  public static String reportBanner(String[] inTitles) {
    StringBuffer theBuffer=new StringBuffer(4096);
    theBuffer.append("\n"+kDivider+"\n");
    for (int i=0; i<inTitles.length; i++)
      theBuffer.append("// "+inTitles[i]+"\n");
    theBuffer.append(kDivider+"\n");
    return theBuffer.toString();
  }

  public static String reportBanner(String inTitle) {
    return reportBanner(new String[] {inTitle}); }
  
//--------------------------------------------------------------------------------------------------------
// reportHeader
//--------------------------------------------------------------------------------------------------------

  public static String reportHeader(String inTitle, long inStartTime) {
    return reportBanner(new String[] {inTitle,"Starting at "+formatTime(inStartTime)})+"\n\n"; }
  
//--------------------------------------------------------------------------------------------------------
// reportFooter
//--------------------------------------------------------------------------------------------------------

  public static String reportFooter(long inStartTime) {
    long theEndTime=System.currentTimeMillis();
    return reportBanner(new String[] {"Done at "+formatTime(theEndTime)+
        "      "+formatDuration(theEndTime-inStartTime)+" elapsed"});
  }
  
}

