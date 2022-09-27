//--------------------------------------------------------------------------------------------------------
// FileUtils.java
//--------------------------------------------------------------------------------------------------------

package hiD.utils;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

//--------------------------------------------------------------------------------------------------------
// FileUtils
//--------------------------------------------------------------------------------------------------------

public class FileUtils implements Constants {

//--------------------------------------------------------------------------------------------------------
// doesFileExist
//--------------------------------------------------------------------------------------------------------

  public static boolean doesFileExist(String inFilename) throws IOException {
    return Files.exists(Paths.get(inFilename)); }

//--------------------------------------------------------------------------------------------------------
// getFilesSize
//--------------------------------------------------------------------------------------------------------

  public static long getFileSize(String inFilename) throws IOException {
    return Files.size(Paths.get(inFilename)); }

//--------------------------------------------------------------------------------------------------------
// deleteFile
//--------------------------------------------------------------------------------------------------------

  public static void deleteFile(String inFilename) throws IOException {
    if (doesFileExist(inFilename)) {
      for (int i=0; i<100; i++) 
        try {
          Files.delete(Paths.get(inFilename));
          break;
        } catch (DirectoryNotEmptyException e) {
          // Wait for file system to catch up when deleting all files in a dir, and then the dir
          try { Thread.sleep(10); } catch (Exception e2) { }
        }      
    }      
  }

//--------------------------------------------------------------------------------------------------------
// openInputStream
//--------------------------------------------------------------------------------------------------------

  public static BufferedInputStream openInputStream(String inFilename) throws IOException {
    return new BufferedInputStream(new FileInputStream(inFilename),kIOBufferSize); }

//--------------------------------------------------------------------------------------------------------
// openInputReader
//--------------------------------------------------------------------------------------------------------

  public static BufferedReader openInputReader(String inFilename) throws IOException {
    return new BufferedReader(new InputStreamReader(openInputStream(inFilename),"UTF-8")); }

//--------------------------------------------------------------------------------------------------------
// loadBinaryFile
//--------------------------------------------------------------------------------------------------------

  public static byte[] loadBinaryFile(String inFilename) throws IOException {
    long theFileLength=getFileSize(inFilename);
    if (theFileLength>Integer.MAX_VALUE/2-1024)
      throw new RuntimeException("File too big:  "+inFilename);
    byte[] theBytes=new byte[(int) theFileLength];
    BufferedInputStream theStream=openInputStream(inFilename);
    try {
      theStream.read(theBytes);
    } finally {
      theStream.close();
    }
    return theBytes; 
  }

//--------------------------------------------------------------------------------------------------------
// loadTextFile
//--------------------------------------------------------------------------------------------------------

  public static String loadTextFile(String inFilename) throws IOException {
    return new String(loadBinaryFile(inFilename),"UTF-8"); }

//--------------------------------------------------------------------------------------------------------
// openOutputStream
//--------------------------------------------------------------------------------------------------------

  public static BufferedOutputStream openOutputStream(String inFilename) throws IOException {
    return new BufferedOutputStream(new FileOutputStream(inFilename),kIOBufferSize); }
  
//--------------------------------------------------------------------------------------------------------
// openOutputWriter
//--------------------------------------------------------------------------------------------------------

  public static BufferedWriter openOutputWriter(String inFilename) throws IOException {
     return new BufferedWriter(new OutputStreamWriter(openOutputStream(inFilename),"UTF-8")); }

//--------------------------------------------------------------------------------------------------------
// saveBinaryFile
//--------------------------------------------------------------------------------------------------------

  public static void saveBinaryFile(byte[] inBytes, String inFilename) throws IOException {
    deleteFile(inFilename);
    BufferedOutputStream theStream=openOutputStream(inFilename);
    try {
      theStream.write(inBytes);
    } finally {
      theStream.flush();
      theStream.close();
    }
  }

//--------------------------------------------------------------------------------------------------------
// saveTextFile
//--------------------------------------------------------------------------------------------------------

  public static void saveTextFile(String inFilename, String inText) throws IOException {
    saveBinaryFile(inText.getBytes("UTF-8"),inFilename); }

//--------------------------------------------------------------------------------------------------------
// createDir
//--------------------------------------------------------------------------------------------------------

  public static void createDir(String inDirname) throws IOException {
    Files.createDirectories(Paths.get(inDirname)); }

//--------------------------------------------------------------------------------------------------------
// listFiles
//--------------------------------------------------------------------------------------------------------

  public static String[] listFiles(String inDirname) throws IOException {
    
    Path theDirPath=Paths.get(inDirname);
    ArrayList<Path> theFilePathList=new ArrayList<Path>();

    // List all paths in current dir
    Stream<Path> theChildPathStream=null;
    try {
      theChildPathStream=Files.list(theDirPath);
      Iterator<Path> theChildPathIterator=theChildPathStream.iterator();
      
      // Loop over paths
      while (theChildPathIterator.hasNext()) {
        Path theChildPath=theChildPathIterator.next();
        
        // Keep files that match filter (if present)
        if (Files.isRegularFile(theChildPath)) 
          theFilePathList.add(theChildPath);
  
        // Ignore symlinks?  Try to avoid using symlinks 
      }
    } finally {
      theChildPathStream.close();
      theChildPathStream=null;
    }
    
    String[] theFilenames=new String[theFilePathList.size()];
    for (int i=0; i<theFilenames.length; i++)
      theFilenames[i]=theDirPath.relativize(theFilePathList.get(i)).toString();
    
    SortUtils.sort(theFilenames,false);
    
    return theFilenames;
  }

}

