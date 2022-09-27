//--------------------------------------------------------------------------------------------------------
// SearchResult.java
//--------------------------------------------------------------------------------------------------------

package hiD.search;

import hiD.data.*;
import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// SearchResult
//--------------------------------------------------------------------------------------------------------

public class SearchResult extends FormatUtils {

//--------------------------------------------------------------------------------------------------------
// SearchResult member vars
//--------------------------------------------------------------------------------------------------------
 
  private DataSet   mDataSet;
  private int       mSearchNNear;
  private boolean   mIncludeDups;
  private int       mQueryDx;
  private float[]   mQueryVector;
  private String    mQueryDescriptor;
  private int[]     mNearVectorDxs;
  private float[]   mNearDistance2s;
  private long      mNDistanceCalcs;

//--------------------------------------------------------------------------------------------------------
// SearchResult 
//--------------------------------------------------------------------------------------------------------
  
  // Completed result
  public SearchResult(
      DataSet     inDataSet,
      int         inSearchNNear,
      boolean     inIncludeDups,
      int         inQueryDx,
      float[]     inQueryVector,
      String      inQueryDescriptor,
      int[]       inNearVectorDxs, 
      float[]     inNearDistance2s, 
      long        inNDistanceCalcs) {
    mDataSet=inDataSet;
    mQueryDx=inQueryDx;
    mQueryVector=inQueryVector;
    mQueryDescriptor=inQueryDescriptor;
    mIncludeDups=inIncludeDups;
    mSearchNNear=inSearchNNear;
    mNearVectorDxs=inNearVectorDxs;
    mNearDistance2s=inNearDistance2s;
    mNDistanceCalcs=inNDistanceCalcs;
  }
  
  // Empty result
  public SearchResult(DataSet inDataSet) {
    this(inDataSet,
         0,
         false,
         kNotFound,
         null,
         null,
         new int[inDataSet.getNVectors()],
         new float[inDataSet.getNVectors()],
         kNotFound);
  }
    
//--------------------------------------------------------------------------------------------------------
// gets
//--------------------------------------------------------------------------------------------------------
  
  public DataSet getDataSet() { return mDataSet; }
  public int getSearchNNear() { return mSearchNNear; }
  public boolean getIncludeDups() { return mIncludeDups; }
  public int getQueryDx() { return mQueryDx; }
  public float[] getQueryVector() { return mQueryVector; }
  public String getQueryDescriptor() { return mQueryDescriptor; }
 
  // Get vectorDx for rank
  public int getNearestVectorDx() { return mNearVectorDxs[0]; }  
  public int getFurthestVectorDx() { return mNearVectorDxs[mSearchNNear-1]; }  
  public int getNearVectorDx(int inRank) { return mNearVectorDxs[inRank]; }  
  public int[] getNearVectorDxs() { return mNearVectorDxs; }  
  
  // Get distance2 for rank
  public float getNearestDistance2() { return mNearDistance2s[0]; }  
  public float getFurthestDistance2() { return mNearDistance2s[mSearchNNear-1]; }  
  public float getNearDistance2(int inRank) { return mNearDistance2s[inRank]; }  
  public float[] getNearDistance2s() { return mNearDistance2s; }  
 
  public long getNDistanceCalcs() { return mNDistanceCalcs; }  

//--------------------------------------------------------------------------------------------------------
// show
//--------------------------------------------------------------------------------------------------------

  public void show() {
    log("\nSearch Result");
    log("  DataSet          "+mDataSet.getStandardFilename());
    log("  SearchNNear:     "+mSearchNNear);
    log("  IncludeDups:     "+mIncludeDups);
    
    String theDescriptor=mQueryDescriptor;
    if (theDescriptor!=null)
      theDescriptor=theDescriptor.substring(theDescriptor.lastIndexOf("/")+1);
    log("\n  QueryVector "+mQueryDx+":  "+(theDescriptor!=null?theDescriptor:""));

    StringBuffer theBuffer=new StringBuffer();
    theBuffer.append("{");
    for (int i=0; i<=Math.min(20,mDataSet.getNDims()); i++) {
      if (i>0)
        theBuffer.append(",");
      theBuffer.append(formatDistance2(mQueryVector[i]));
    }
    if (mDataSet.getNDims()>20)
      theBuffer.append(",...");
    theBuffer.append("}");
    log("    "+theBuffer.toString());

    boolean theHasDescriptors=mDataSet.getHasDescriptors();
    log("\n"+
        leftPad("Near",10)+
        leftPad("Vector",10)+
        leftPad("Distance",10));
    log(leftPad("Neighbor",10)+
        leftPad("Index",10)+
        leftPad("Squared",10)+
        leftPad("Distance",10)+
        "      "+(theHasDescriptors?"Vector Descriptor":""));
    for (int i=0; i<mSearchNNear; i++) {
      theDescriptor=mDataSet.getDescriptor(mNearVectorDxs[i]);
      if (theDescriptor!=null)
        theDescriptor=theDescriptor.substring(theDescriptor.lastIndexOf("/")+1);
      log(leftPad(i+1,10)+
          leftPad(mNearVectorDxs[i],10)+
          leftPad(formatDistance2(mNearDistance2s[i]),10)+
          leftPad(formatDistance2(Math.sqrt(mNearDistance2s[i])),10)+
          "      "+((theDescriptor!=null)?theDescriptor:""));
    }
    log("  NDistanceCalcs:  "+mNDistanceCalcs);
  }

}

