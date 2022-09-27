//--------------------------------------------------------------------------------------------------------
// DataSet.java
//--------------------------------------------------------------------------------------------------------

package hiD.data;

import java.io.*;
import java.util.HashMap;

import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// DataSet
//--------------------------------------------------------------------------------------------------------

public class DataSet extends FormatUtils {
  
//--------------------------------------------------------------------------------------------------------
// DataSet member vars
//--------------------------------------------------------------------------------------------------------

  private int         mNDims;              // Number of dimensions
  private int         mNVectors;           // Number of vectors in dataset
  private String      mSourceName;         // Source name
  private double      mMaxLengthScale;     // Length of longest vector
  private double      mStdLengthScale;     // Expected length of vectors is √NDims
  private boolean     mHasDescriptors;     // Boolean flag whether vectors have a descriptor string
  
  private float[]     mMean;               // Subtracted out first to center data
  private double      mScale;              // Applied second to scale so the expected variance is 1 per dimension

  // Vector data
  private float[][]   mVectors;
  //   1st array index is the vector index
  //   2nd array is the vector component index

  // Descriptor data
  private String[]    mDescriptors;        // Optional field, may be null

  // Derived fields - calculated when needed, then kept
  private float[]     mVectorLengths;      
  private HashMap     mDescriptorLookup;

//--------------------------------------------------------------------------------------------------------
// DataSet 
//--------------------------------------------------------------------------------------------------------
  
  public DataSet(
      int         inNDims, 
      int         inNVectors,
      String      inSourceName,
      double      inMaxLengthScale,
      float[]     inMean,
      double      inScale,
      float[][]   inVectors,
      String[]    inDescriptors) {
    mNDims=inNDims;
    mNVectors=inNVectors;
    mSourceName=inSourceName;
    mMaxLengthScale=inMaxLengthScale;
    mStdLengthScale=Math.sqrt(inNDims);
    mHasDescriptors=(inDescriptors!=null);
    mMean=inMean;
    mScale=inScale;
    mVectors=inVectors;
    mDescriptors=inDescriptors;
  }  

//--------------------------------------------------------------------------------------------------------
// gets
//--------------------------------------------------------------------------------------------------------
  
  public int getNDims() { return mNDims; }  
  public int getNVectors() { return mNVectors; }  
  public String getSourceName() { return mSourceName; }    
  
  // Standard filename looks like:  Source_NDims_NVecs.vecs  as in  OpenI_512D_1Mv.vecs
  public String getStandardFilename() { return standardDataSetFilename(mSourceName,mNDims,mNVectors); }  
  
  public double getStdLengthScale() { return mStdLengthScale; }
  public double getStdLengthScale2() { return mStdLengthScale*mStdLengthScale; }
  public double getMaxLengthScale() { return mMaxLengthScale; }
  public double getMaxLengthScale2() { return mMaxLengthScale*mMaxLengthScale; }
  
  public float[] getMean() { return mMean; }  
  public double getScale() { return mScale; }

  // Due to need for speed (and save RAM), original vector arrays returned - not copies
  // Any changes to returned vectors will change DataSet
  public float[] getVector(int inVectorDx) { return mVectors[inVectorDx]; }  
  public float[][] getVectors() { return mVectors; }  
  
  public boolean getHasDescriptors() { return mHasDescriptors; }
  public String getDescriptor(int inVectorDx) { 
    return (mHasDescriptors?mDescriptors[inVectorDx]:null); }  
  public String[] getDescriptors() { return mDescriptors; }  

//--------------------------------------------------------------------------------------------------------
// getVectorLengths
//--------------------------------------------------------------------------------------------------------

  public float[] getVectorLengths() {
    if (mVectorLengths==null) {
      mVectorLengths=new float[mNVectors];
      for (int i=0; i<mNVectors; i++)
        mVectorLengths[i]=(float) VectorUtils.vectorLength(getVector(i));
    }
    return mVectorLengths;
  }
  
  public float getVectorLength(int inVectorDx) { return getVectorLengths()[inVectorDx]; }

//--------------------------------------------------------------------------------------------------------
// getDescriptorLookup
//--------------------------------------------------------------------------------------------------------

  public HashMap getDescriptorLookup() {
    if (mDescriptorLookup==null) {
      mDescriptorLookup=new HashMap(mNVectors);
      for (int i=0; i<mNVectors; i++)
        mDescriptorLookup.put(mDescriptors[i],Integer.valueOf(i));
    }
    return mDescriptorLookup;
  }
  
  public int getVectorDxForDescriptor(String inDescriptor) { 
    Integer theInteger=(Integer) getDescriptorLookup().get(inDescriptor);
    if (theInteger==null)
      return kNotFound;
    else
      return theInteger.intValue(); 
  }

//--------------------------------------------------------------------------------------------------------
// save
//--------------------------------------------------------------------------------------------------------

  public void save() throws IOException {
    
    String theFilename=kDataSetDir+"/"+getStandardFilename();
    log("\nSaving DataSet in "+theFilename);
    
    BufferedOutputStream theStream=FileUtils.openOutputStream(theFilename);
    try {
      
      byte[] theBytes=new byte[1024];

      // Write header params
      ConversionUtils.longToBytes(kNotFound,theBytes,0);             // Unused - was RandomSeed, but no longer generating random data
      theStream.write(theBytes,0,ConversionUtils.kLongMemory);
      
      ConversionUtils.intToBytes(mNDims,theBytes,0);                 // NDims
      theStream.write(theBytes,0,ConversionUtils.kIntMemory);   
      
      ConversionUtils.intToBytes(mNVectors,theBytes,0);              // NVectors
      theStream.write(theBytes,0,ConversionUtils.kIntMemory);
      
      byte[] theSourceNameBytes=getSourceName().getBytes("UTF-8");   // SourceName - a short UTF8 identifier string with a leading length
      short theSourceNameMemory=(short) theSourceNameBytes.length;
      ConversionUtils.shortToBytes(theSourceNameMemory,theBytes,0);
      theStream.write(theBytes,0,ConversionUtils.kShortMemory);
      theStream.write(theSourceNameBytes,0,theSourceNameMemory);
      
      ConversionUtils.doubleToBytes(mMaxLengthScale,theBytes,0);     // Max length scale = length of longest vector
      theStream.write(theBytes,0,ConversionUtils.kDoubleMemory);

      ConversionUtils.booleanToBytes(mHasDescriptors,theBytes,0);    // HasDescriptors - boolean  
      theStream.write(theBytes,0,ConversionUtils.kBooleanMemory);
      
      int theVectorMemory=mNDims*ConversionUtils.kFloatMemory;
      if (theVectorMemory>theBytes.length)
        theBytes=new byte[theVectorMemory];
      
      // When a dataset is created, it is centered and scaled so that the expected variance is 1 per dimension (i.e. √NDims overall)
      // This is just to simpify interpretation, and has no real impact on indexing or search
      // The mean and scale are included in the dataset so that the original data can be recovered if desired

      ConversionUtils.floatsToBytes(mMean,0,mNDims,theBytes,0);      // Mean vector subtracted out to center
      theStream.write(theBytes,0,theVectorMemory);
      
      ConversionUtils.doubleToBytes(mScale,theBytes,0);              // Scale applied so that the expected variance is 1 per dimension
      theStream.write(theBytes,0,ConversionUtils.kDoubleMemory);
      
      // Get and write vectors and descriptors
      for (int i=0; i<mNVectors; i++) {
        
        // Write vector 
        ConversionUtils.floatsToBytes(mVectors[i],0,mNDims,theBytes,0);                   // Vector
        theStream.write(theBytes,0,theVectorMemory);
        
        // If DataSet has descriptors, 
        if (mHasDescriptors) {
          
          // If the vector is missing its descriptor ...
          if (mDescriptors[i]==null) {
            
            // Write descriptor length of 0 
            ConversionUtils.shortToBytes((short) 0,theBytes,0);                           // Descriptor length of zero
            theStream.write(theBytes,0,ConversionUtils.kShortMemory);
            
          } else {
            
            // Convert descriptor string into UTF-8 bytes
            byte[] theDescriptorBytes=mDescriptors[i].getBytes("UTF-8");
            
            // Write descriptor length
            ConversionUtils.shortToBytes((short) theDescriptorBytes.length,theBytes,0);   // Descriptor length 
            theStream.write(theBytes,0,ConversionUtils.kShortMemory);

            // Write descriptor bytes
            theStream.write(theDescriptorBytes);                                          // Descriptor
          }
        }
      }

    } finally {
      theStream.flush();
      theStream.close();
    }
    
    long theFileSize=FileUtils.getFileSize(theFilename);
    log("  "+mNDims+" dims, "+mNVectors+" vectors, "+formatMemory(theFileSize)+" on disk");
  }

//--------------------------------------------------------------------------------------------------------
// load
//--------------------------------------------------------------------------------------------------------
  
  public static DataSet load(String inDataSetFilename) throws IOException {
        
    // First look for filename as given
    String theFilename=inDataSetFilename;
    if (!FileUtils.doesFileExist(theFilename)) {
      // Then standardize directory and file extension and look again
      theFilename=kDataSetDir+"/"+stripFilePathAndType(theFilename)+".vecs";    
      if (!FileUtils.doesFileExist(theFilename)) 
        throw new RuntimeException("DataSet does not exist: "+inDataSetFilename);
    }
    
    long theFileSize=FileUtils.getFileSize(theFilename);
    log("\nLoading DataSet "+theFilename);
    
    DataSet theDataSet=null;
    BufferedInputStream theStream=FileUtils.openInputStream(theFilename); 
    try {

      byte[] theBytes=new byte[1024];
      
      // Read header = NDims, NVectors, SourceName
      theStream.read(theBytes,0,ConversionUtils.kLongMemory);                 // Unused - was RandomSeed, but no longer generating random data
      
      theStream.read(theBytes,0,ConversionUtils.kIntMemory);                  // NDims
      int theNDims=ConversionUtils.bytesToInt(theBytes,0);
      
      theStream.read(theBytes,0,ConversionUtils.kIntMemory);                  // NVectors
      int theNVectors=ConversionUtils.bytesToInt(theBytes,0);
      
      theStream.read(theBytes,0,ConversionUtils.kShortMemory);                // SourceName - a short UTF8 identifier string with a leading length
      short theSourceNameMemory=ConversionUtils.bytesToShort(theBytes,0);
      theStream.read(theBytes,0,theSourceNameMemory);
      String theSourceName=new String(theBytes,0,theSourceNameMemory,"UTF-8");
      
      // Read dataset stats
      theStream.read(theBytes,0,ConversionUtils.kDoubleMemory);               // Max length scale = length of longest vector
      double theMaxLengthScale=ConversionUtils.bytesToDouble(theBytes,0);

      theStream.read(theBytes,0,ConversionUtils.kBooleanMemory);              // HasDescriptors - boolean  
      boolean theHasDescriptors=ConversionUtils.bytesToBoolean(theBytes,0);
      
      int theVectorMemory=theNDims*ConversionUtils.kFloatMemory;
      if (theVectorMemory>theBytes.length)
        theBytes=new byte[theVectorMemory];
      
      // When a dataset is created, it is centered and scaled so that the expected variance is 1 per dimension (i.e. √MDims overall)
      // This is just to simpify interpretation, and has no real impact on indexing or search
      // The mean and scale are included in the dataset so that the original data can be recovered if desired

      float[] theMean=new float[theNDims];                                       // Mean vector subtracted out to center
      theStream.read(theBytes,0,theVectorMemory);
      ConversionUtils.bytesToFloats(theBytes,0,theVectorMemory,theMean,0);
      
      theStream.read(theBytes,0,ConversionUtils.kDoubleMemory);                  // Scale applied so that the expected variance is 1 per dimension
      double theScale=ConversionUtils.bytesToDouble(theBytes,0);

      // Allocate space for vectors and descriptors (if available)
      float[][] theVectors=new float[theNVectors][theNDims];
      String[] theDescriptors=(theHasDescriptors?new String[theNVectors]:null);
           
      // Read and set vectors and descriptors
      for (int i=0; i<theNVectors; i++) {
        
        // Read vector
        theStream.read(theBytes,0,theVectorMemory);
        ConversionUtils.bytesToFloats(theBytes,0,theVectorMemory,theVectors[i],0);

        // If DataSet has descriptors, 
        if (theHasDescriptors) {
          
          // Read descriptor length
          theStream.read(theBytes,0,ConversionUtils.kShortMemory);
          short theDescriptorMemory=ConversionUtils.bytesToShort(theBytes,0);
          
          // If descriptor length is zero, leave descriptor null
          // Otherwise, read descriptor bytes and convert to String
          if (theDescriptorMemory>0) {
            
            if (theDescriptorMemory>theBytes.length)
              theBytes=new byte[2*theDescriptorMemory];
            
            theStream.read(theBytes,0,theDescriptorMemory);
            theDescriptors[i]=new String(theBytes,0,theDescriptorMemory,"UTF-8");
          }
        }
      }
      
      // Create DataSet
      theDataSet=new DataSet(
          theNDims,
          theNVectors,
          theSourceName,
          theMaxLengthScale,
          theMean,
          theScale,
          theVectors,
          theDescriptors);
    
    } finally {
      theStream.close();
    }
    
    log("  "+theDataSet.getNDims()+" dims, "+theDataSet.getNVectors()+" vectors, "+
        formatMemory(theFileSize)+" on disk");
    
    return theDataSet;
  }

}

