//--------------------------------------------------------------------------------------------------------
// Index.java
//--------------------------------------------------------------------------------------------------------

package hiD.index;

import java.io.*;
import java.util.ArrayList;

import hiD.data.*;
import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// Index
//--------------------------------------------------------------------------------------------------------

public class Index extends FormatUtils {

//--------------------------------------------------------------------------------------------------------
// Index member vars
//--------------------------------------------------------------------------------------------------------
  
  private DataSet         mDataSet;               // A reference to the indexed dataset 

  private int             mIndexNNear;            // Number of nearest neighbors in index, which can be different from number in search
  
  // Some stats determined during indexing
  private long            mTotNLinks;             // Total number of links in indxe
  private long            mIndexingTime;          // Time spent indexing in millis
  private float           mMaxLinkDistance2;      // Length of longest link

  // The index consists of a list of links for each vector in the dataset
  // Links are sorted from shortest to longest for each vector
  // The first mIndexNNear links are nearest neighbor links and are used primarily in the spread phase of search
  // The remaining links are longer and are used primarily in the descend phase of search
  
  // 2D arrays hold the link data 
  private int[][]         mLinkVectorDxss;        // Destination vector indexes
  private float[][]       mLinkDistance2ss;       // Distance squared between source and destination vectors 
  //   1st array index is the source vector index
  //   2nd array index is the link index for that source vector

//--------------------------------------------------------------------------------------------------------
// Index 
//--------------------------------------------------------------------------------------------------------
  
  public Index(
      DataSet     inDataSet,
      int         inIndexNNear,
      long        inIndexingTime,
      int[][]     inVectorDxss,
      float[][]   inDistance2ss) {
    mDataSet=inDataSet;
    mIndexNNear=inIndexNNear;
    mIndexingTime=inIndexingTime;
    mLinkVectorDxss=inVectorDxss;
    mLinkDistance2ss=inDistance2ss;
    int theNVectors=mDataSet.getNVectors();
    for (int i=0; i<theNVectors; i++) {
      mTotNLinks+=inVectorDxss[i].length;
      mMaxLinkDistance2=Math.max(mMaxLinkDistance2,inDistance2ss[i][inDistance2ss[i].length-1]);
    }
  }

//--------------------------------------------------------------------------------------------------------
// gets - from DataSet
//--------------------------------------------------------------------------------------------------------

  public DataSet getDataSet() { return mDataSet; }
  
  public int getNDims() { return mDataSet.getNDims(); }
  public int getNVectors() { return mDataSet.getNVectors(); }
  public String getSourceName() { return mDataSet.getSourceName(); }

  public float[] getVector(int inVectorDx) { return mDataSet.getVector(inVectorDx); }
  public float[][] getVectors() { return mDataSet.getVectors(); }  
  public boolean getHasDescriptors() { return mDataSet.getHasDescriptors(); }
  public String getDescriptor(int inVectorDx) { return mDataSet.getDescriptor(inVectorDx); }  
  public String[] getDescriptors() { return mDataSet.getDescriptors(); }  
  public int getVectorDxForDescriptor(String inDescriptor) { return mDataSet.getVectorDxForDescriptor(inDescriptor); }  

//--------------------------------------------------------------------------------------------------------
// gets - index specific
//--------------------------------------------------------------------------------------------------------

  // Standard filename looks like:  Source_NDims_NVecs_NNear.ndx  as in  OpenI_512D_1Mv_60Nr.ndx
  public String getStandardFilename() { return standardIndexFilename(getDataSet().getStandardFilename(),mIndexNNear); }

  public int getIndexNNear() { return mIndexNNear; }
  public long getIndexingTime() { return mIndexingTime; }
  public float getMaxLinkDistance2() { return mMaxLinkDistance2; }
  public int getNLinks(int inVectorDx) { return mLinkVectorDxss[inVectorDx].length; }
  public long getTotalNLinks() { return mTotNLinks; }
  public double getAvgNLinks() { return mTotNLinks/(double) getNVectors(); }

  // Due to need for speed (and save RAM), original link arrays returned - not copies
  // Any changes to returned links will change Index
  public int[] getLinkVectorDxs(int inVectorDx) { return mLinkVectorDxss[inVectorDx]; }
  public int[][] getLinkVectorDxss() { return mLinkVectorDxss; }
  public int getNearestLinkVectorDx(int inVectorDx) { return mLinkVectorDxss[inVectorDx][0]; }
  public int getFurthestLinkVectorDx(int inVectorDx) { return mLinkVectorDxss[inVectorDx][mLinkVectorDxss[inVectorDx].length-1]; }
  
  public float[] getLinkDistance2s(int inVectorDx) { return mLinkDistance2ss[inVectorDx]; }
  public float[][] getLinkDistance2ss() { return mLinkDistance2ss; }
  public float getNearestLinkDistance2(int inVectorDx) { return mLinkDistance2ss[inVectorDx][0]; }
  public float getFurthestLinkDistance2(int inVectorDx) { return mLinkDistance2ss[inVectorDx][mLinkDistance2ss[inVectorDx].length-1]; }

  public boolean getIsDup(int inVectorDx) { return ((getNLinks(inVectorDx)==1)&&(getNearestLinkDistance2(inVectorDx)==0)); }

//--------------------------------------------------------------------------------------------------------
// listAvailableIndexes
//--------------------------------------------------------------------------------------------------------

  public static String[] listAvailableIndexes(DataSet inDataSet) throws Exception {
    String[] theFilenames=FileUtils.listFiles(kIndexDir);
    String theIndexPrefix=stripFilePathAndType(inDataSet.getStandardFilename());
    ArrayList theIndexFilenameList=new ArrayList();
    for (int i=0; i<theFilenames.length; i++) 
      if (theFilenames[i].startsWith(theIndexPrefix))
        theIndexFilenameList.add(theFilenames[i]);
    return (String[]) theIndexFilenameList.toArray(new String[theIndexFilenameList.size()]);
  }

//--------------------------------------------------------------------------------------------------------
// save
//--------------------------------------------------------------------------------------------------------

  public void save() throws IOException {
    
    String theFilename=kIndexDir+"/"+getStandardFilename();
    log("\nSaving Index in "+theFilename);
    
    BufferedOutputStream theStream=FileUtils.openOutputStream(theFilename);
    try {
      
      byte[] theBytes=new byte[1024];

      // Write header params
      ConversionUtils.longToBytes(kNotFound,theBytes,0);
      theStream.write(theBytes,0,ConversionUtils.kLongMemory);       // Unused - was RandomSeed, but no longer generating random data
      
      ConversionUtils.intToBytes(getNDims(),theBytes,0);             // NDims
      theStream.write(theBytes,0,ConversionUtils.kIntMemory);
      
      int theNVectors=getNVectors();                                 // NVectors
      ConversionUtils.intToBytes(theNVectors,theBytes,0);
      theStream.write(theBytes,0,ConversionUtils.kIntMemory);
      
      byte[] theSourceNameBytes=getSourceName().getBytes("UTF-8");   // SourceName - a short UTF8 identifier string with a leading length
      short theSourceNameMemory=(short) theSourceNameBytes.length;
      ConversionUtils.shortToBytes(theSourceNameMemory,theBytes,0);
      theStream.write(theBytes,0,ConversionUtils.kShortMemory);
      theStream.write(theSourceNameBytes,0,theSourceNameMemory);
      
      ConversionUtils.intToBytes(mIndexNNear,theBytes,0);            // IndexNNear - number of nearest neighbors built into index
      theStream.write(theBytes,0,ConversionUtils.kIntMemory);

      ConversionUtils.longToBytes(mIndexingTime,theBytes,0);         // IndexingTime - time spent indexing in millis
      theStream.write(theBytes,0,ConversionUtils.kLongMemory);
      
      theBytes=new byte[theNVectors*ConversionUtils.kIntMemory];
      theStream.write(theBytes);                                     // Unused - was CreateVectorDxs
      theStream.write(theBytes);                                     // Unused - was CreateDistance2s

      // Get and write links for each vector
      for (int i=0; i<theNVectors; i++) {
        
        // Write number of links 
        int theNLinks=mLinkVectorDxss[i].length;                                      // NLinks
        ConversionUtils.intToBytes(theNLinks,theBytes,0);
        theStream.write(theBytes,0,ConversionUtils.kIntMemory);
        
        // Write linked vector indexes
        ConversionUtils.intsToBytes(mLinkVectorDxss[i],0,theNLinks,theBytes,0);       // Link vector indexes
        theStream.write(theBytes,0,ConversionUtils.kIntMemory*theNLinks);
        
        // Write link distance squareds
        ConversionUtils.floatsToBytes(mLinkDistance2ss[i],0,theNLinks,theBytes,0);    // Link distance squareds
        theStream.write(theBytes,0,ConversionUtils.kFloatMemory*theNLinks);
      }

    } finally {
      theStream.flush();
      theStream.close();
    }
    
    long theFileSize=FileUtils.getFileSize(theFilename);
    log("  "+mIndexNNear+" near, "+formatMemory(theFileSize)+" on disk");
  }

//--------------------------------------------------------------------------------------------------------
// load
//--------------------------------------------------------------------------------------------------------
  
  // Version that uses a DataSet that is already available in RAM - avoids DataSet reload
  public static Index load(DataSet inDataSet, String inIndexFilename) throws IOException {

    // First look for filename as given
    String theFilename=inIndexFilename;
    if (!FileUtils.doesFileExist(theFilename)) {
      // Then standardize directory and file extension and look again
      theFilename=kIndexDir+"/"+stripFilePathAndType(theFilename)+".ndx";
      if (!FileUtils.doesFileExist(theFilename))
        throw new RuntimeException("Index does not exist: "+inIndexFilename);
    }
    
    // Confirm index filename matches dataset filename
    String theDataSetFilename=extractIndexDataSetFilename(inIndexFilename);
    if (!inDataSet.getStandardFilename().equals(theDataSetFilename))
      throw new RuntimeException("Index does not match dataset: "+inIndexFilename);

    long theFileSize=FileUtils.getFileSize(theFilename);
    log("\nLoading Index "+theFilename);
    
    Index theIndex=null;
    BufferedInputStream theStream=FileUtils.openInputStream(theFilename); 
    try {
      
      byte[] theBytes=new byte[1024];

      // Read header = NDims, NVectors, SourceName
      theStream.read(theBytes,0,ConversionUtils.kLongMemory);                // Unused - was RandomSeed, but no longer generating random data
      
      theStream.read(theBytes,0,ConversionUtils.kIntMemory);                 // NDims
      int theNDims=ConversionUtils.bytesToInt(theBytes,0);

      theStream.read(theBytes,0,ConversionUtils.kIntMemory);                 // NVectors
      int theNVectors=ConversionUtils.bytesToInt(theBytes,0);
      
      theStream.read(theBytes,0,ConversionUtils.kShortMemory);               // SourceName - a short UTF8 identifier string with a leading length
      short theSourceNameMemory=ConversionUtils.bytesToShort(theBytes,0);
      theStream.read(theBytes,0,theSourceNameMemory);
      String theSourceName=new String(theBytes,0,theSourceNameMemory,"UTF-8");
      
      // Confirm dataset compatibility
      if (theNDims!=inDataSet.getNDims())
        throw new RuntimeException("DataSet has different NDims");
      if (theNVectors!=inDataSet.getNVectors())
        throw new RuntimeException("DataSet has different NVectors");
      if (!theSourceName.equals(inDataSet.getSourceName()))
        throw new RuntimeException("DataSet has different SourceName");

      // Read index stats
      theStream.read(theBytes,0,ConversionUtils.kIntMemory);                 // IndexNNear - number of nearest neighbors built into index
      int theIndexNNear=ConversionUtils.bytesToInt(theBytes,0);

      theStream.read(theBytes,0,ConversionUtils.kLongMemory);      
      long theIndexingTime=ConversionUtils.bytesToLong(theBytes,0);          // Indexing time - time spent indexing in millis
      
      theBytes=new byte[theNVectors*ConversionUtils.kIntMemory];
      theStream.read(theBytes);                                              // Unused CreateVectorDxs
      theStream.read(theBytes);                                              // Unused CreateDistance2s
      
      int[][] theLinkVectorDxss=new int[theNVectors][];
      float[][] theLinkDistance2ss=new float[theNVectors][];
      
      // Read and set links for each vector
      for (int i=0; i<theNVectors; i++) {
        
        // Read number of links 
        theStream.read(theBytes,0,ConversionUtils.kIntMemory);                  // NLinks
        int theNLinks=ConversionUtils.bytesToInt(theBytes,0);
        
        // Read linked vector indexes
        theLinkVectorDxss[i]=new int[theNLinks];
        theStream.read(theBytes,0,ConversionUtils.kIntMemory*theNLinks);
        ConversionUtils.bytesToInts(theBytes,0,theNLinks*ConversionUtils.kIntMemory,theLinkVectorDxss[i],0);

        // Read link distance squareds
        theLinkDistance2ss[i]=new float[theNLinks];
        theStream.read(theBytes,0,ConversionUtils.kFloatMemory*theNLinks);
        ConversionUtils.bytesToFloats(theBytes,0,theNLinks*ConversionUtils.kFloatMemory,theLinkDistance2ss[i],0);
     }
   
      // Create Index
      theIndex=new Index(
          inDataSet,
          theIndexNNear,   
          theIndexingTime,
          theLinkVectorDxss,
          theLinkDistance2ss);    
      
    } finally {
      theStream.close();
    }
    
    log("  "+theIndex.getIndexNNear()+" near, "+theIndex.getTotalNLinks()+" links, "+
        formatMemory(theFileSize)+" on disk, "+formatDuration(theIndex.getIndexingTime())+" to index");
    
    return theIndex;
  }
  
  
  public static Index load(String inIndexFilename) throws IOException {
    DataSet theDataSet=DataSet.load(kDataSetDir+"/"+extractIndexDataSetFilename(inIndexFilename));
    return load(theDataSet,inIndexFilename);
  }

}

