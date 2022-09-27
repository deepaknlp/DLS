//--------------------------------------------------------------------------------------------------------
// BruteSearch.java
//--------------------------------------------------------------------------------------------------------

package hiD.search;

import hiD.data.*;
import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// BruteSearch
//--------------------------------------------------------------------------------------------------------

public class BruteSearch extends FormatUtils {
  
//--------------------------------------------------------------------------------------------------------
// BruteSearch member vars
//--------------------------------------------------------------------------------------------------------

  private DataSet       mDataSet;
  private int           mSearchNNear;
  private boolean       mIncludeDups;
  
  private float[]       mMeasuredDistance2s;   // Keeps track of all measured distances
  private Accumulator   mAccumulator;

//--------------------------------------------------------------------------------------------------------
// BruteSearch 
//--------------------------------------------------------------------------------------------------------
  
  public BruteSearch(DataSet inDataSet, int inSearchNNear, boolean inIncludeDups) {
    mDataSet=inDataSet;
    mSearchNNear=inSearchNNear;
    mIncludeDups=inIncludeDups;
    int theNVectors=mDataSet.getNVectors();
    mMeasuredDistance2s=new float[theNVectors];
    mAccumulator=new Accumulator(mSearchNNear,mMeasuredDistance2s);
  }

//--------------------------------------------------------------------------------------------------------
// gets
//--------------------------------------------------------------------------------------------------------

  public DataSet getDataSet() { return mDataSet; }
  public int getSearchNNear() { return mSearchNNear; }
  public boolean getIncludeDups() { return mIncludeDups; }
  
//--------------------------------------------------------------------------------------------------------
// search
// 
// Assumes search is reused in a single thread - create multiple BruteSearch objects to multi-thread
//--------------------------------------------------------------------------------------------------------
 
  // This method reuses a search result to avoid reallocating big arrays - useful when testing with 10,000 searches
  public SearchResult search(
      int            inQueryDx, 
      float[]        inQueryVector, 
      String         inQueryDescriptor,
      SearchResult   inSearchResult) {

    // Clean up 
    for (int i=0; i<mMeasuredDistance2s.length; i++) 
      mMeasuredDistance2s[i]=kNotFound;
    mAccumulator.reset();

    // Loop over all data vectors
    int theNVectors=mDataSet.getNVectors();
    for (int theVectorDx=0; theVectorDx<theNVectors; theVectorDx++) {
      
      // Calc distance to query vector
      float[] theVector=mDataSet.getVector(theVectorDx);
      float theDistance2=(float) VectorUtils.vectorSeparation2(inQueryVector,theVector);
      mMeasuredDistance2s[theVectorDx]=theDistance2;
      
      // Accumulate vector in search result
      if (mIncludeDups) 
        mAccumulator.addVectorDx(theVectorDx);
      
      // If not including dups, check whether new calc is a dup 
      // Quick fail if distance longer than all vectors in the accumulator
      else if (theDistance2<=mAccumulator.getNearLimitDistance2()) {
        
        // Note that for equal distances, the kept vector is the one with the smaller vectorDx.
        // Since brute checks vectors in order of vectorDx, we can skip the equality case in the prev line

        // Loop over vectors in the accumulator 
        boolean theIsDup=false;
        int theNInHeap=mAccumulator.getNNear();
        for (int i=0; i<theNInHeap; i++)
          
          // If distance the same, check if vectors are dups
          if (theDistance2==mAccumulator.getDistance2(i)) {
            
            // Testing for dup is faster than a distance calc because you can fail early
            theIsDup=VectorUtils.vectorsAreDups(mDataSet.getVector(mAccumulator.getVectorDx(i)),theVector);
            if (theIsDup) 
              break;
          }
        
        // If not a dup, accumulate vector in search result
        if (!theIsDup)
          mAccumulator.addVectorDx(theVectorDx);
      }      
    }
    
    // Reuse arrays from search result 
    int[] theNearVectorDxs=inSearchResult.getNearVectorDxs();
    float[] theNearDistance2s=inSearchResult.getNearDistance2s();
    
    // Get nearest neighbor vectors from accumulator 
    int theSearchNNear=mAccumulator.removeNear(theNearVectorDxs);
    // Copy distances
    for (int i=0; i<theSearchNNear; i++) 
      theNearDistance2s[i]=mMeasuredDistance2s[theNearVectorDxs[i]];
 
    return new SearchResult( 
        mDataSet,
        theSearchNNear,
        mIncludeDups,
        inQueryDx,
        inQueryVector,
        inQueryDescriptor,
        theNearVectorDxs,
        theNearDistance2s,
        theNVectors);
  }


  // This method allocates and returns a new search result
  public SearchResult search(
      int            inQueryDx, 
      float[]        inQueryVector, 
      String         inQueryDescriptor) {
    return search(
        inQueryDx,
        inQueryVector,
        inQueryDescriptor,
        new SearchResult(mDataSet)); }

//--------------------------------------------------------------------------------------------------------
// Inner class SearchThread
//--------------------------------------------------------------------------------------------------------

  private static class SearchThread extends Thread {
    
    // Member vars
    BruteSearch       mBruteSearch;
    SearchResultSet   mSearchResultSet;
    int               mStartQueryDx;
    int               mEndQueryDx;
    long              mElapsedTime;
    double            mSumDistance2;

    // Constructor 
    public SearchThread(
        BruteSearch       inBruteSearch,
        SearchResultSet   inSearchResultSet,
        int               inStartQueryDx,
        int               inEndQueryDx) {
      mBruteSearch=inBruteSearch;
      mSearchResultSet=inSearchResultSet;
      mStartQueryDx=inStartQueryDx;
      mEndQueryDx=inEndQueryDx;
      setDaemon(true);
    }
 
    // Code that does the work
    public void run() {
      
      DataSet theQuerySet=mSearchResultSet.getQuerySet();

      // Loop over queries
      long theStartTime=System.currentTimeMillis();
      for (int i=mStartQueryDx; i<mEndQueryDx; i++) {
        float[] theQueryVector=theQuerySet.getVector(i);
        String theQueryDescriptor=theQuerySet.getDescriptor(i);
        SearchResult theSearchResult=mSearchResultSet.getSearchResult(i);

        // Calc true nearest neighbors by Brute force
        theSearchResult=mBruteSearch.search(
            i,
            theQueryVector,
            theQueryDescriptor,
            theSearchResult);  
        mSumDistance2+=theSearchResult.getNearestDistance2();
      }
      mElapsedTime=System.currentTimeMillis()-theStartTime;
    }
  };

//--------------------------------------------------------------------------------------------------------
// searchSet
//--------------------------------------------------------------------------------------------------------

  public static final int    kNThreads=Math.min(kNCores-2,(int) (kNCores*0.8));

  public static SearchResultSet searchSet(SearchResultSet inSearchResultSet) {
  
    // Create threads
    int theNQueries=inSearchResultSet.getQuerySet().getNVectors();
    SearchThread[] theThreads=new SearchThread[kNThreads];    
    for (int i=0; i<kNThreads; i++) {
      
      BruteSearch theBruteSearch=new BruteSearch(
          inSearchResultSet.getDataSet(),
          inSearchResultSet.getSearchNNear(),
          inSearchResultSet.getIncludeDups());
      
      int theStartQueryDx=(i*theNQueries)/kNThreads;
      int theEndQueryDx=((i+1)*theNQueries)/kNThreads;
      theThreads[i]=new SearchThread(
          theBruteSearch,
          inSearchResultSet,
          theStartQueryDx,
          theEndQueryDx);
    }
    
    // Run threads
    try {
      for (int i=0; i<kNThreads; i++) 
        theThreads[i].start();
      for (int i=0; i<kNThreads; i++) 
        theThreads[i].join();
    } catch (Throwable e) {
      throw new RuntimeException("SearchThread died",e);
    }

    long theElapsedThreadTime=0;
    double theSumDistance2=0;
    for (int i=0; i<kNThreads; i++) {
      SearchThread theThread=theThreads[i];
      theElapsedThreadTime+=theThread.mElapsedTime;
      theSumDistance2+=theThread.mSumDistance2;
      theThreads[i]=null;
    }
    double theAvgNearestDistance2=theSumDistance2/theNQueries;
    double theAvgTimePerQuery=theElapsedThreadTime/(double) theNQueries;

    log("\n  Avg Search Time:        "+formatDuration(theAvgTimePerQuery)+" per query");
    log("  Avg Nearest Distance2:  "+formatDistance2((float) theAvgNearestDistance2));

    SearchResultSet theSearchResultSet=new SearchResultSet(
        inSearchResultSet,
        inSearchResultSet.getDataSet().getNVectors(),
        theAvgNearestDistance2,
        theAvgTimePerQuery);
    
    return theSearchResultSet;
  }
 
  
  public static SearchResultSet searchSet(
      DataSet   inDataSet,
      int       inSearchNNear,
      boolean   inIncludeDups,
      DataSet   inQuerySet) {
    
    // Create SearchResultSet
    SearchResultSet theSearchResultSet=new SearchResultSet(
        inDataSet,
        inSearchNNear,
        inIncludeDups,
        inQuerySet);

    return searchSet(theSearchResultSet);
  }

}

