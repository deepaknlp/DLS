//--------------------------------------------------------------------------------------------------------
// SearchResultSet.java
//--------------------------------------------------------------------------------------------------------

package hiD.search;

import hiD.data.*;
import hiD.index.*;
import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// SearchResultSet
//--------------------------------------------------------------------------------------------------------

public class SearchResultSet extends FormatUtils {
  
//--------------------------------------------------------------------------------------------------------
// SearchResultSet member vars
//--------------------------------------------------------------------------------------------------------

  private Index       mIndex;
  private DataSet     mDataSet;
  private int         mSearchNNear;
  private boolean     mIncludeDups;
  private DataSet     mQuerySet;
  
  private int[][]     mNearVectorDxss;
  private float[][]   mNearDistance2ss;
  
  private long        mAvgNDistanceCalcs;
  private double      mAvgNearestDistance2;
  private double      mAvgTimePerQuery;

//--------------------------------------------------------------------------------------------------------
// SearchResultSet 
//--------------------------------------------------------------------------------------------------------
   
  private SearchResultSet(
      Index       inIndex,
      DataSet     inDataSet,
      int         inSearchNNear,
      boolean     inIncludeDups,
      DataSet     inQuerySet) {
    
    mIndex=inIndex;
    mDataSet=inDataSet;
    mSearchNNear=inSearchNNear;
    mIncludeDups=inIncludeDups;
    mQuerySet=inQuerySet;
    
    mNearVectorDxss=new int[inQuerySet.getNVectors()][inSearchNNear];
    mNearDistance2ss=new float[inQuerySet.getNVectors()][inSearchNNear];
  }  

  public SearchResultSet(
      Index       inIndex,
      int         inSearchNNear,
      boolean     inIncludeDups,
      DataSet     inQuerySet) {
    this(inIndex,
         inIndex.getDataSet(),
         inSearchNNear,
         inIncludeDups,
         inQuerySet);
  }  

  public SearchResultSet(
      DataSet     inDataSet,
      int         inSearchNNear,
      boolean     inIncludeDups,
      DataSet     inQuerySet) {
    this(null,
         inDataSet,
         inSearchNNear,
         inIncludeDups,
         inQuerySet);
  }  

  public SearchResultSet(
      SearchResultSet   inSearchResultSet,
      long              inAvgNDistanceCalcs,
      double            inAvgNearestDistance2,
      double            inTimePerQuery) {
    mIndex=inSearchResultSet.mIndex;
    mDataSet=inSearchResultSet.mDataSet;
    mSearchNNear=inSearchResultSet.mSearchNNear;
    mIncludeDups=inSearchResultSet.mIncludeDups;
    mQuerySet=inSearchResultSet.mQuerySet;
    mNearVectorDxss=inSearchResultSet.mNearVectorDxss;
    mNearDistance2ss=inSearchResultSet.mNearDistance2ss;
    
    mAvgNDistanceCalcs=inAvgNDistanceCalcs;
    mAvgNearestDistance2=inAvgNearestDistance2;
    mAvgTimePerQuery=inTimePerQuery;
  }  

//--------------------------------------------------------------------------------------------------------
// gets
//--------------------------------------------------------------------------------------------------------
  
  public Index getIndex() { return mIndex; }
  public DataSet getDataSet() { return mDataSet; }
  public boolean getWasBruteForceSearch() { return (mIndex==null); }
  public int getSearchNNear() { return mSearchNNear; }
  public boolean getIncludeDups() { return mIncludeDups; }
  public DataSet getQuerySet() { return mQuerySet; }
  
  public long getAvgNDistanceCalcs() { return mAvgNDistanceCalcs; }
  public double getAvgNearestDistance2() { return mAvgNearestDistance2; }
  public double getAvgTimePerQuery() { return mAvgTimePerQuery; }

  public int getNDims() { return mDataSet.getNDims(); }  
  public int getNDataVectors() { return mDataSet.getNVectors(); }  
  public int getNQueryVectors() { return mQuerySet.getNVectors(); }  
  public String getSourceName() { return mDataSet.getSourceName(); }    
  
  public SearchResult getSearchResult(int inQueryDx) { 
    return new SearchResult(
        mDataSet,
        mSearchNNear,
        mIncludeDups,
        inQueryDx,
        mQuerySet.getVector(inQueryDx),
        mQuerySet.getDescriptor(inQueryDx),
        mNearVectorDxss[inQueryDx], 
        mNearDistance2ss[inQueryDx], 
        kNotFound);
   }  

}

