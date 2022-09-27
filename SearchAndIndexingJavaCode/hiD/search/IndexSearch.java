//--------------------------------------------------------------------------------------------------------
// IndexSearch.java
//--------------------------------------------------------------------------------------------------------

package hiD.search;

import hiD.data.*;
import hiD.index.*;
import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// IndexSearch
//--------------------------------------------------------------------------------------------------------

public class IndexSearch extends FormatUtils {

//--------------------------------------------------------------------------------------------------------
// IndexSearch consts
//--------------------------------------------------------------------------------------------------------

  private static final boolean   kVerbose=false;
    
  // Each vector in the index gets a set of flags
  private static final byte      kNoFlags=0;
  private static final byte      kMeasuredFlag=16;     // Indicates distance to query has been measured
  private static final byte      kSpreadFlag=32;       // Indicates node has been spread - all links have been measured
  
  // Speeds up calc ~10% - not sure worth the added complexity - currently turned off
  private static final boolean   kUseReferenceCountsInSpread=false;
  private static final int       kReferenceCountThreshold=3;
  private static final byte      kReferenceMask=15;    // Bottom 4bits of flags hold reference count 

//--------------------------------------------------------------------------------------------------------
// IndexSearch member vars
//--------------------------------------------------------------------------------------------------------

  private Index         mIndex;
  private int           mSearchNNear;              // Number of nearest neighbors in search, Ks, which can be different from number in index, Ki
  private boolean       mIncludeDups;

  private int           mQueryDx;                  // Index of query vector - just used for reporting purposes
  private float[]       mQueryVector;
  private String        mQueryDescriptor;          // For OpenI data, this is the URL of the image that produced the feature vector

  private long          mNDescends;
  private long          mNSpreads;
  private long          mNDescendCalcs;
  private long          mNSpreadCalcs;
  private int           mNMeasuredVectors;
  private int[]         mMeasuredVectorDxs;
  private float[]       mMeasuredDistance2s;
  private byte[]        mVectorFlags;
  private int[]         mNearVectorDxs;            // Work array to copy nodeDxs out of accumulator
  private Accumulator   mAccumulator;              // heap that keeps track of the K nearest nodes

//--------------------------------------------------------------------------------------------------------
// IndexSearch 
//--------------------------------------------------------------------------------------------------------
  
  public IndexSearch(Index inIndex, int inSearchNNear, boolean inIncludeDups) {
    mIndex=inIndex;
    mSearchNNear=inSearchNNear;
    mIncludeDups=inIncludeDups;
    
    int theNVectors=mIndex.getNVectors();
    mMeasuredVectorDxs=new int[theNVectors];
    mMeasuredDistance2s=new float[theNVectors];
    mVectorFlags=new byte[theNVectors];
    mNearVectorDxs=new int[theNVectors];
    mNMeasuredVectors=0;
    for (int i=0; i<theNVectors; i++) {
      mMeasuredVectorDxs[i]=kNotFound;
      mMeasuredDistance2s[i]=kNotFound;
      mVectorFlags[i]=kNoFlags;
      mNearVectorDxs[i]=kNotFound;
    }
    mAccumulator=new Accumulator(mSearchNNear,mMeasuredDistance2s);
  }

//--------------------------------------------------------------------------------------------------------
// gets
//--------------------------------------------------------------------------------------------------------

  public Index getIndex() { return mIndex; }
  public DataSet getDataSet() { return mIndex.getDataSet(); }
  public int getSearchNNear() { return mSearchNNear; }
  public boolean getIncludeDups() { return mIncludeDups; }

//--------------------------------------------------------------------------------------------------------
// search
//
// Assumes search is reused in a single thread - create multiple IndexSearch objects to multi-thread
//--------------------------------------------------------------------------------------------------------

  // This method reuses a search result
  public SearchResult search(
      int            inQueryDx,
      float[]        inQueryVector, 
      String         inQueryDescriptor,
      SearchResult   inSearchResult) {
    start(inQueryDx,inQueryVector,inQueryDescriptor);      
    step();
    if (mIncludeDups)
      addDups();    
    return done(mIncludeDups,inSearchResult);
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
        new SearchResult(mIndex.getDataSet())); }

//--------------------------------------------------------------------------------------------------------
// Inner class SearchThread
//--------------------------------------------------------------------------------------------------------

  private static class SearchThread extends Thread {
    
    // Member vars
    IndexSearch       mIndexSearch;
    SearchResultSet   mSearchResultSet;
    int               mStartQueryDx;
    int               mEndQueryDx;
    long              mNDistanceCalcs;
    long              mElapsedTime;
    double            mSumDistance2;

    // Constructor 
    public SearchThread(
        IndexSearch       inIndexSearch,
        SearchResultSet   inSearchResultSet, 
        int               inStartQueryDx,
        int               inEndQueryDx) {
      mIndexSearch=inIndexSearch;
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

        // Calc nearest neighbors 
        theSearchResult=mIndexSearch.search(
            i,
            theQueryVector,
            theQueryDescriptor,
            theSearchResult);  
        mNDistanceCalcs+=theSearchResult.getNDistanceCalcs();
        mSumDistance2+=theSearchResult.getNearestDistance2();
      }
      mElapsedTime=System.currentTimeMillis()-theStartTime;
    }
  };

//--------------------------------------------------------------------------------------------------------
// searchSet
// 
// Assumes search is reused in a single thread - create multiple BruteSearch objects to multi-thread
//--------------------------------------------------------------------------------------------------------

  public static final int    kNThreads=Math.min(kNCores-2,(int) (kNCores*0.8));

  public static SearchResultSet searchSet(SearchResultSet inSearchResultSet) {
    
    // Create threads
    int theNQueries=inSearchResultSet.getQuerySet().getNVectors();
    SearchThread[] theThreads=new SearchThread[kNThreads];    
    for (int i=0; i<kNThreads; i++) {
      
      IndexSearch theIndexSearch=new IndexSearch(
          inSearchResultSet.getIndex(),
          inSearchResultSet.getSearchNNear(),
          inSearchResultSet.getIncludeDups());

      int theStartQueryDx=(i*theNQueries)/kNThreads;
      int theEndQueryDx=((i+1)*theNQueries)/kNThreads;
      theThreads[i]=new SearchThread(
          theIndexSearch,
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

    long theSumNDistanceCalcs=0;
    long theElapsedThreadTime=0;
    double theSumDistance2=0;
    for (int i=0; i<kNThreads; i++) {
      SearchThread theThread=theThreads[i];
      theSumNDistanceCalcs+=theThread.mNDistanceCalcs;
      theElapsedThreadTime+=theThread.mElapsedTime;
      theSumDistance2+=theThread.mSumDistance2;
      theThreads[i]=null;
    }
    long theAvgNDistanceCalcs=theSumNDistanceCalcs/theNQueries;
    double theAvgNearestDistance2=theSumDistance2/theNQueries;
    double theAvgTimePerQuery=theElapsedThreadTime/(double) theNQueries;

    log("\n  Avg Search Time:        "+formatDuration(theAvgTimePerQuery)+" per query");
    log("  Avg N Distance Calcs:   "+theAvgNDistanceCalcs+" per query    "+
        formatPercent(theAvgNDistanceCalcs/(double) inSearchResultSet.getDataSet().getNVectors())+" of Brute");
    log("  Avg Nearest Distance2:  "+formatDistance2((float) theAvgNearestDistance2));

    SearchResultSet theSearchResultSet=new SearchResultSet(
        inSearchResultSet,
        theAvgNDistanceCalcs,
        theAvgNearestDistance2,
        theAvgTimePerQuery);

    return theSearchResultSet;
  }

  
  
  public static SearchResultSet searchSet(
      Index     inIndex,
      int       inSearchNNear,
      boolean   inIncludeDups,
      DataSet   inQuerySet) {
    
    // Create SearchResultSet
    SearchResultSet theSearchResultSet=new SearchResultSet(
        inIndex,
        inSearchNNear,
        inIncludeDups,
        inQuerySet);

    return searchSet(theSearchResultSet);
  }
  
  
  
  
//--------------------------------------------------------------------------------------------------------
// The rest of the class is search routines, which are internal implementation details and have private access
//--------------------------------------------------------------------------------------------------------

//--------------------------------------------------------------------------------------------------------
// start
//--------------------------------------------------------------------------------------------------------

  private void start(int inQueryDx, float[] inQueryVector, String inQueryDescriptor) {
    
    if (mQueryVector!=null)
      throw new RuntimeException("Search already in use");
    
    mQueryDx=inQueryDx;
    mQueryVector=inQueryVector;
    mQueryDescriptor=inQueryDescriptor;
  
    // Clean up 
    for (int i=0; i<mNMeasuredVectors; i++) {
      int theVectorDx=mMeasuredVectorDxs[i];
      mMeasuredVectorDxs[i]=kNotFound;
      mMeasuredDistance2s[theVectorDx]=kNotFound;
      mVectorFlags[theVectorDx]=kNoFlags;
    }
    mNMeasuredVectors=0;
    mAccumulator.reset();

    mNDescends=0;
    mNSpreads=0;
    mNDescendCalcs=0;
    mNSpreadCalcs=0;
    mNMeasuredVectors=0;
  }
 
//--------------------------------------------------------------------------------------------------------
// calcDistance2
//--------------------------------------------------------------------------------------------------------
  
  private float calcDistance2(int inVectorDx, boolean inDescendCalc) {
    
    // This is a recent optimization 
    // Speeds up calcs significantly, at cost of less accurate search results
    // During spread, we calculate on the order of K^2 distance calcs, which is 10,000 for our standard K=100
    // One way to avoid many of these calcs is to only calc distance for vectors linked to several of the K nearest neighbors
    // So we count how many links reference a vector and wait till the count exceeds some threshold before doing the distance calc
    boolean theDoCalc=true;
    if (kUseReferenceCountsInSpread)
      if (!inDescendCalc) {   // Always do calcs during descend
        int theReferenceCount=(mVectorFlags[inVectorDx]&kReferenceMask)+1;  // includes +1 reference for this vector
        mVectorFlags[inVectorDx]=(byte) ((mVectorFlags[inVectorDx]&(~kReferenceMask))|theReferenceCount);
        theDoCalc=(theReferenceCount>=kReferenceCountThreshold);            // do calc when count exceeds threshold
      }
    
    if (theDoCalc) {
      float theMeasuredDistance2=(float) VectorUtils.vectorSeparation2(mIndex.getVector(inVectorDx),mQueryVector);
      mMeasuredVectorDxs[mNMeasuredVectors++]=inVectorDx;       // Track which vectors have been measured, so never measure again,
      mMeasuredDistance2s[inVectorDx]=theMeasuredDistance2;     //   and their measured distance2s
      mVectorFlags[inVectorDx]|=kMeasuredFlag;                  // Flag vector as measured

      // Track number of calcs in descend and spread 
      // Calcs are the majority of search time, so want to minimize them
      // Currently, spread dominates for standard 100NN case, so thats where optimizations make a difference
      if (inDescendCalc)
        mNDescendCalcs++;
      else
        mNSpreadCalcs++;

      return theMeasuredDistance2;
    } else 
      return kNotFound;  // Indicates distance not calculated - only occurs when using ref counts in spread
  }
  
//--------------------------------------------------------------------------------------------------------
// bullseye
//--------------------------------------------------------------------------------------------------------
  
  private void bullseye() {

    // Found query at distance zero, indicating it is a node from the indexed dataset
    // Copy over node links as the search result
    int theNearestVectorDx=mAccumulator.getNearestVectorDx();
 
    // Get links from index
    int theNLinks=mIndex.getNLinks(theNearestVectorDx);
    int[] theLinkVectorDxs=mIndex.getLinkVectorDxs(theNearestVectorDx);
    float[] theLinkDistance2s=mIndex.getLinkDistance2s(theNearestVectorDx);
    
    // Loop over links for current node (shortest to longest)
    for (int j=0; j<theNLinks; j++) {
      int theLinkVectorDx=theLinkVectorDxs[j];
      float theLinkDistance2=theLinkDistance2s[j];            

      // Skip dups (zero length links)
      // Dups added in at end after stepping done
      if (theLinkDistance2>0) {
        
        // Don't keep links that are too big to matter
        // Bail - remaining links bigger
        if (theLinkDistance2>mAccumulator.getNearLimitDistance2()) 
          break;

        // Skip links to known nodes - they are already in the accumulator
        if (mMeasuredDistance2s[theLinkVectorDx]==kNotFound) {
          
          // No need for calc - node to query distance is link distance
          mMeasuredVectorDxs[mNMeasuredVectors++]=theLinkVectorDx;
          mMeasuredDistance2s[theLinkVectorDx]=theLinkDistance2;  
          mVectorFlags[theLinkVectorDx]|=kMeasuredFlag;

          // Update near nodes
          mAccumulator.addVectorDx(theLinkVectorDx);
        }
      }
    }
  }
  
//--------------------------------------------------------------------------------------------------------
// descend
//   checks the distances (to query) for links of the first nearest neighbor
//   allows search to rapidly step from the root vector to some vector near the query
//   will continue to be called as long as the nearest neighbor keeps changing
//   when the nearest neighbor stops changing, spread will be called instead
//--------------------------------------------------------------------------------------------------------
  
  private void descend() {
    
    mNDescends++;
    long theStartNCalcs=mNDescendCalcs;

    int theNearestVectorDx=mAccumulator.getNearestVectorDx();
    
    // Links that are too long are less likely to make progress and are skipped
    // This heuristic is based loosely on the geometry of overlapping hyper spheres, but the fine structure 
    //   of the data also has a big impact.  More reasearch & experiments needed to optimize 
    float theQueryDistance2=mMeasuredDistance2s[theNearestVectorDx];
    float theMaxLinkDistance2=theQueryDistance2;

    // Another optimization is to follow the first long link that makes significant progress
    // This shortcut takes advantage of the fact the overall distribution is only one std dev thick
    // If ever a link makes 3 std dev of progress, we are unlikely to do better so skip rest of the links
    // Note that spread takes more than 10x longer than descend for std 100NN case, so speeding up descend 
    //   does not make much difference
    double theShortCircuitDistance=Math.max(0,Math.sqrt(theQueryDistance2)-3);
    float theShortCircuitDistance2=(float) (theShortCircuitDistance*theShortCircuitDistance);

    // Get links from index
    int theNLinks=mIndex.getNLinks(theNearestVectorDx);
    int[] theLinkVectorDxs=mIndex.getLinkVectorDxs(theNearestVectorDx);
    float[] theLinkDistance2s=mIndex.getLinkDistance2s(theNearestVectorDx);

    // Loop over links for current node, longest to shortest
    for (int j=theNLinks-1; j>=0; j--) {
      float theLinkDistance2=theLinkDistance2s[j];
      
      // Don't follow links that are too big to find a closer neighbor
      if (theLinkDistance2>theMaxLinkDistance2) 
        continue;

      // Skip dups (zero length links)
      // Dups are optionally added in at end, after stepping is done
      if (theLinkDistance2==0) 
        break;   // Bail - remaining links are shorter (also dups)
      
      // Skip links to known nodes - they are already in the accumulator
      int theLinkVectorDx=theLinkVectorDxs[j];
      if (mMeasuredDistance2s[theLinkVectorDx]==kNotFound) {

        // Calc distance from linked node to query vector 
        float theMeasuredDistance2=calcDistance2(theLinkVectorDx,true);
       
        // Update nodes in accumulator
        mAccumulator.addVectorDx(theLinkVectorDx);
        
        // Early nodes, especially the root node, have tons of links
        // If we follow a link that makes significant progress, bail out of loop over the rest of them
        if (theMeasuredDistance2<theShortCircuitDistance2)
          break;
      }              
    }
    
    if (kVerbose) {
      long theStepNCalcs=mNDescendCalcs-theStartNCalcs;
      System.out.println("  Descend "+mNDescends+",  "+theStepNCalcs+" calcs");
    }
    
    // Return to step which will decide whether to do another descend or a spread
  }
  
//--------------------------------------------------------------------------------------------------------
// spread
//   checks the distances (to query) for links of all K nearest neighbors
//   allows search to use nearest neighbors of indexed vectors to spread out and find nearest neighbors
//     of the query vector
//   will continue to be called as long as any of the K near neighbors keeps changing
//   if the nearest neighbor changes, descend will be called instead
//   if none of the K near neighbors change, search is done
//--------------------------------------------------------------------------------------------------------
  
  private void spread() {
    
    mNSpreads++;
    long theStartNCalcs=mNSpreadCalcs;
    
    // The kept limit node is the furthest of the near nodes = top of the heap
    // When the kept node changes, it indicates a new near node has been found
    int theKeptLimitVectorDx=mAccumulator.getNearLimitVectorDx();
    
    // Loop over near neighbor nodes 
    // We will break out of loop when a new near node is found 
    // Heap is only partially sorted, with furthest of the near node at the top of the heap (index zero)
    // Looping backwards through heap roughly gets nearer nodes first
    int theNNear=mAccumulator.getNNear();
    for (int i=theNNear-1; i>=0; i--) {
      
      int theNearVectorDx=mAccumulator.getVectorDx(i);
      float theQueryDistance2=mAccumulator.getDistance2(i);

      long theInnerStartNCalcs=mNSpreadCalcs;
      
      // Don't spread nodes which have already been spread (i.e. all links checked)
      // This lets us race past finished nodes to find the remaining unspread ones 
      if ((mVectorFlags[theNearVectorDx]&kSpreadFlag)!=0) 
        continue;
      
      // Links that are too long are less likely to make progress and are skipped
      // This heuristic is based loosely on the geometry of overlapping hyper spheres, but the fine structure 
      //   of the data also has a big impact.  More reasearch & experiments needed to optimize 
      float theKeptLimitDistance2=mAccumulator.getNearLimitDistance2();
      float theMaxLinkDistance=2.2f*theKeptLimitDistance2-theQueryDistance2;      // If heuristic changed, change in ### 2nd place below
   
      // Get links from index
      int theNLinks=mIndex.getNLinks(theNearVectorDx);
      int[] theLinkVectorDxs=mIndex.getLinkVectorDxs(theNearVectorDx);
      float[] theLinkDistance2s=mIndex.getLinkDistance2s(theNearVectorDx);
  
      // Loop over links for current near node, shortest to longest
      for (int j=0; j<theNLinks; j++) {
        float theLinkDistance2=theLinkDistance2s[j];
  
        // Skip dups (zero length links)
        // Dups are optionally added in at end, after stepping is done
        if (theLinkDistance2==0) 
          continue;
          
        // Don't follow links that are too big to find a closer neighbor
        if (theLinkDistance2>theMaxLinkDistance) 
          break;  // Bail - remaining links are longer
        
        // Skip links to known nodes - they have already been added to the accumulator
        int theLinkVectorDx=theLinkVectorDxs[j];
        if (mMeasuredDistance2s[theLinkVectorDx]==kNotFound) {

          // Calc distance from linked node to query vector 
          // ### This line takes 90% of search time for std 100NN case ###
          float theMeasuredDistance2=calcDistance2(theLinkVectorDx,false);

          // New optimization: spread does not perform calc until several links reference it
          // Only continue if calc actually performed
          if (theMeasuredDistance2!=kNotFound) {
            
            // Update nodes in accumulator
            boolean theFoundNearNode=(mAccumulator.addVectorDx(theLinkVectorDx)!=kNotFound);

            // If we found a near node
            if (theFoundNearNode) {
              
              // Update kept distance and max link distance so we can bail out of spread loop earlier
              theKeptLimitDistance2=mAccumulator.getNearLimitDistance2();
              theMaxLinkDistance=2.2f*theKeptLimitDistance2-theQueryDistance2;        // ### 2nd place mentioned above
            }
          }                           
        }
      }
      
      // All useful links measured
      // Mark the current node as spread, so it won't be spread again
      mVectorFlags[theNearVectorDx]|=kSpreadFlag;
      
      if (kVerbose) {
        long theInnerStepNCalcs=mNSpreadCalcs-theInnerStartNCalcs;
        System.out.println("    Spread "+mNSpreads+"."+i+",  "+theInnerStepNCalcs+" calcs");
      }
      
      // If new near node was found, break out of spread and return to step
      // Then step will decide if search is done or whether do a descend or another spread
      if (theKeptLimitVectorDx!=mAccumulator.getNearLimitVectorDx()) 
        break;
    }
    
    if (kVerbose) {
      long theStepNCalcs=mNSpreadCalcs-theStartNCalcs;
      System.out.println("  Spread "+mNSpreads+",  "+theStepNCalcs+" calcs");
    }
  }
  
//--------------------------------------------------------------------------------------------------------
// step
//--------------------------------------------------------------------------------------------------------
  
  private void step() {

    // Start at root node
    // Note: using the word node and vector interchangeably
    int theRootVectorDx=0;    

    // The accumulator is a heap that keeps track of the nodes near the query
    // At the end of the search, the accumulator contents will be the search result - i.e. the K nearest neighbors
    // Start with adding the root node to the accumulator
    // ### CAUTION!  Calc distance first!  ###
    // For efficiency, the accumulator shares an array of distances with this search class and assumes the
    //   distance has already been calculated when a vector is added
    calcDistance2(theRootVectorDx,true);
    mAccumulator.addVectorDx(theRootVectorDx);

    // The kept limit node is the furthest of the near nodes = top of the accumulator heap
    // When the kept node changes, it indicates a new near node has been added to the accumulator
    int theKeptLimitVectorDx=kNotFound;
    
    // The nearest node is the closest of the near nodes - tracked by accumulator, separately from heap
    // When the nearest node changes, it indicates a descend step should be taken, otherwise a spread
    int theNearestVectorDx=kNotFound;
    
    // Descend can get stuck in local minima, but spread finds a way out and the algorithm descends again
    // Must make sure to do a spread after descend is complete
    // Search not finished without a spread

    // Repeatedly step until spread does not change the K nearest nodes
    while (true) {
 
      // Rare case
      // If nearest node to query distance is zero, query was one of the indexed vectors
      // No need to calc distances from linked nodes to query - link lengths are the distances 
      if (mAccumulator.getNearestDistance2()==0) {
        bullseye();
        return;  // Search is done      
           
      // If the nearest node changed, we can make rapid progress with a descend step
      } else if (theNearestVectorDx!=mAccumulator.getNearestVectorDx()) {
        theNearestVectorDx=mAccumulator.getNearestVectorDx();
        descend();
     
      // Otherwise, do a spread step
      } else {
        spread();
        
        // Check if we are done
        // If kept limit node did not change, then none of them did, and we have as many NN as can be found
        if (theKeptLimitVectorDx==mAccumulator.getNearLimitVectorDx())
          break;
        else
          theKeptLimitVectorDx=mAccumulator.getNearLimitVectorDx();
      }
    }
  }

//--------------------------------------------------------------------------------------------------------
// addDups
//--------------------------------------------------------------------------------------------------------

  private void addDups() {
    
    // Duplicates (aka dups) corrupt nearest neighbor searches
    // If a vector has K+1 dups, a K nearest neighbor search finds nothing interesting
    // During indexing, duplicates are found, separated from the nearest neighbor links, and added back in at the end
    // If J vectors are the same, one is treated as the real vector and the other J-1 are dups.
    // Dups are recognized by having one zero length link to the real vector

    int theSearchNNear=mAccumulator.copyNear(mNearVectorDxs);
    
    // Loop over near nodes
    for (int i=0; i<theSearchNNear; i++) {        
      int theVectorDx=mNearVectorDxs[i]; 
      float theMeasuredDistance2=mMeasuredDistance2s[theVectorDx];
      
      // Get links
      int theNLinks=mIndex.getNLinks(theVectorDx);
      int[] theLinkVectorDxs=mIndex.getLinkVectorDxs(theVectorDx);
      float[] theLinkDistance2s=mIndex.getLinkDistance2s(theVectorDx);
             
      // Loop over links
      for (int j=0; j<theNLinks; j++) {
        
        // Dups at start - bail when link not a dup
        if (theLinkDistance2s[j]>0)
          break;

        // Don't add if already measured - already in accumulator
        int theLinkVectorDx=theLinkVectorDxs[j];
        if (mMeasuredDistance2s[theLinkVectorDx]==kNotFound) {
          mMeasuredVectorDxs[mNMeasuredVectors++]=theLinkVectorDx;
          mMeasuredDistance2s[theLinkVectorDx]=theMeasuredDistance2;  
          mVectorFlags[theLinkVectorDx]|=kMeasuredFlag;
          mAccumulator.addVectorDx(theLinkVectorDx);
          
          if (theNLinks==1) 
            throw new RuntimeException("Found dup added real");
          
        }
      }
    }
  }

//--------------------------------------------------------------------------------------------------------
// done
//--------------------------------------------------------------------------------------------------------

  private SearchResult done(boolean inIncludeDups, SearchResult ioSearchResult) {
    
    // For efficiency, reuse the search result arrays for near neighbors and their distances from query
    int[] theNearVectorDxs=ioSearchResult.getNearVectorDxs();
    float[] theNearDistance2s=ioSearchResult.getNearDistance2s();
    
    // Get near neighbors from accumulator in sorted order - this resets the accumulator 
    int theSearchNNear=mAccumulator.removeNear(theNearVectorDxs);
    
    // Copy out distances
    theNearDistance2s[0]=mMeasuredDistance2s[theNearVectorDxs[0]];
    for (int i=1; i<theSearchNNear; i++) 
      theNearDistance2s[i]=mMeasuredDistance2s[theNearVectorDxs[i]];
 
    int theQueryDx=mQueryDx;
    float[] theQueryVector=mQueryVector;
    String theQueryDescriptor=mQueryDescriptor;
    
    mQueryDx=kNotFound;
    mQueryVector=null;
    mQueryDescriptor=null;
    
    SearchResult theSearchResult=new SearchResult( 
        mIndex.getDataSet(),
        theSearchNNear,
        inIncludeDups,
        theQueryDx,
        theQueryVector,
        theQueryDescriptor,
        theNearVectorDxs,
        theNearDistance2s,
        mNDescendCalcs+mNSpreadCalcs);
    
    if (kVerbose) {
      System.out.println("Search summary:");
      System.out.println("  Total of "+(mNDescendCalcs+mNSpreadCalcs)+" calcs");
      System.out.println("  "+mNDescends+" descends   "+mNDescendCalcs+" calcs  "+
          formatPercent(mNDescendCalcs/(double) (mNDescendCalcs+mNSpreadCalcs)));
      System.out.println("  "+mNSpreads+" spreads   "+mNSpreadCalcs+" calcs  "+
          formatPercent(mNSpreadCalcs/(double) (mNDescendCalcs+mNSpreadCalcs)));
    }

    return theSearchResult;
  }
  
}

