//--------------------------------------------------------------------------------------------------------
// IndexVector.java
//--------------------------------------------------------------------------------------------------------

package hiD.index;

import java.util.Vector;
import java.util.concurrent.*;

import hiD.data.*;
import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// IndexVector
//--------------------------------------------------------------------------------------------------------

public class IndexVector extends FormatUtils {
  
  
  
//--------------------------------------------------------------------------------------------------------
// This class has several static class vars and routines (top) as well as full featured 
//   instances (below) with their own member vars and routines
//--------------------------------------------------------------------------------------------------------
 
//--------------------------------------------------------------------------------------------------------
// IndexVector class vars
//--------------------------------------------------------------------------------------------------------
  
  private static CreateHeap      gCreateHeap;
  private static DataSet         gDataSet;
  private static int             gIndexNNear;
  
  private static byte[]          gVectorFlags; 
  private static IndexVector[]   gIndexVectors;
  private static float[]         gNearDistance2s;   // ***
  
  // *** Could be taken from top link in each vector's heap, but keeping a copy separately for multi-threading

  private static long            gNCalcs;
  private static long            gNUsefulCalcs;
  private static long            gNLinksKept;
  private static float           gAvgNearDistance2;  
  private static float           gShortcutDistance2;  
  
  private static int             gNDups;
  private static int[]           gDupVectorDxs;
  private static int[]           gDupOfVectorDxs;   

//--------------------------------------------------------------------------------------------------------
// open 
//--------------------------------------------------------------------------------------------------------
  
  public static void open(CreateHeap inCreateHeap, int inIndexNNear) {
    gCreateHeap=inCreateHeap;
    gDataSet=inCreateHeap.getDataSet();
    gIndexNNear=inIndexNNear;

    gNCalcs=0;
    gNUsefulCalcs=0;
    gAvgNearDistance2=Float.MAX_VALUE;
    gShortcutDistance2=1.5f*gDataSet.getNDims();
    gNDups=0;

    int theNVectors=gDataSet.getNVectors();
    gVectorFlags=new byte[theNVectors];
    gIndexVectors=new IndexVector[theNVectors];
    gNearDistance2s=new float[theNVectors];
    gDupVectorDxs=new int[theNVectors];
    gDupOfVectorDxs=new int[theNVectors];
    for (int i=0; i<theNVectors; i++) {
      gVectorFlags[i]=kNoFlags;
      gIndexVectors[i]=new IndexVector(i);
      gNearDistance2s[i]=Float.MAX_VALUE;;
      gDupVectorDxs[i]=kNotFound;
      gDupOfVectorDxs[i]=kNotFound;
    }
  }
  
//--------------------------------------------------------------------------------------------------------
// close 
//--------------------------------------------------------------------------------------------------------
  
  public static void close() {
    gCreateHeap=null;
    gDataSet=null;
    gVectorFlags=null;
    gIndexVectors=null;
    gNearDistance2s=null;
  }

//--------------------------------------------------------------------------------------------------------
// getIndexVector
//--------------------------------------------------------------------------------------------------------

  public static IndexVector getIndexVector(int inVectorDx) { return gIndexVectors[inVectorDx]; }

//--------------------------------------------------------------------------------------------------------
// get NCalcs
//--------------------------------------------------------------------------------------------------------
 
  public static long getNCalcs() { return gNCalcs; }
  public static long getNUsefulCalcs() { return gNUsefulCalcs; }
  public static long getNLinksKept() { return gNLinksKept; }
 
//--------------------------------------------------------------------------------------------------------
// calcAvgNearDistance2
//--------------------------------------------------------------------------------------------------------

  public static float calcAvgNearDistance2() {
    double theTotNearDistance2=0;
    int theNContributing=1;
    int theNVectors=gDataSet.getNVectors();
    for (int i=0; i<theNVectors; i++) 
      if ((gVectorFlags[i]&kIsNodeFlag)==0) {
        theTotNearDistance2+=gNearDistance2s[i];
        theNContributing++;
      }
    gAvgNearDistance2=(float) (theTotNearDistance2/theNContributing);
    return gAvgNearDistance2;
  }

//--------------------------------------------------------------------------------------------------------
// get Dups
//--------------------------------------------------------------------------------------------------------
 
  public static int getNDups() { return gNDups; }
  public static int[] getDupVectorDxs() { return gDupVectorDxs; }
  public static int[] getDupOfVectorDxs() { return gDupOfVectorDxs; }

  
  
  

//--------------------------------------------------------------------------------------------------------
// IndexVector consts
//--------------------------------------------------------------------------------------------------------

  private static final int       kNInChunk=256;   // 8 byte links, so chunk is 2K RAM
  private static final int       kMinNChunks=4;   // Space for holding chunks, chunks may or may not be allocated

//--------------------------------------------------------------------------------------------------------
// IndexVector member vars
//--------------------------------------------------------------------------------------------------------
  
  private int         mVectorDx;
  
  // Links - bottom NNear in heap, rest list
  private int[][]     mOtherVectorDxss; 
  private float[][]   mDistance2ss;
  
  // Heap
  private int         mNInHeap;
  private int         mHeadFreeLinkDx;
  private int[]       mHeapLinkDxs;       // Need indirection so can remove links to dups
  private int[]       mLinkHeapDxs;       // Also used as next ptr for free list 

  // List
  private int         mNInList;

  // Dups
  private int         mDupVectorDx1;
  private int         mDupVectorDx2;

  // Links kept for output to final index
  private int[]       mFarLinkVectorDxs;
  private float[]     mFarLinkDistance2s;
  private int[]       mNearLinkVectorDxs;
  private float[]     mNearLinkDistance2s;
 
//--------------------------------------------------------------------------------------------------------
// IndexVector 
//--------------------------------------------------------------------------------------------------------
  
  public IndexVector(int inVectorDx) {
    
    mVectorDx=inVectorDx;
    
    // Start with space to hold a minimal N of chunks
    mOtherVectorDxss=new int[kMinNChunks][];
    mDistance2ss=new float[kMinNChunks][];
    // Start with only one chunk allocated
    mOtherVectorDxss[0]=new int[kNInChunk];
    mDistance2ss[0]=new float[kNInChunk];
    for (int i=0; i<kNInChunk; i++) {
      mOtherVectorDxss[0][i]=kNotFound;
      mDistance2ss[0][i]=kNotFound;
    }
    
    mNInHeap=0;
    mHeapLinkDxs=new int[gIndexNNear];
    mLinkHeapDxs=new int[gIndexNNear];
    for (int i=0; i<gIndexNNear; i++) {
      mHeapLinkDxs[i]=kNotFound;
      mLinkHeapDxs[i]=i+1;
    }
    mLinkHeapDxs[gIndexNNear-1]=kNotFound;
    mHeadFreeLinkDx=0;
    
    mNInList=0;
    
    mDupVectorDx1=kNotFound;
    mDupVectorDx2=kNotFound;
  }

//--------------------------------------------------------------------------------------------------------
// gets for reading out results after all links are found
//--------------------------------------------------------------------------------------------------------

  public int[] getFarLinkVectorDxs() { return mFarLinkVectorDxs; }
  public float[] getFarLinkDistance2s() { return mFarLinkDistance2s; }
  public int[] getNearLinkVectorDxs() { return mNearLinkVectorDxs; }
  public float[] getNearLinkDistance2s() { return mNearLinkDistance2s; }
  
//--------------------------------------------------------------------------------------------------------
// keepLinks
//--------------------------------------------------------------------------------------------------------

  public void keepLinks(boolean inFar) {
 
    // Collect links from the heap
    int theNLinks=0;
    int[] theLinkVectorDxs=new int[mNInHeap];
    float[] theLinkDistance2s=new float[mNInHeap];
    for (int i=mNInHeap-1; i>=0; i--) {
      int theHeapLinkDx=mHeapLinkDxs[i];
      int theOtherVectorDx=mOtherVectorDxss[0][theHeapLinkDx];
      if ((gVectorFlags[theOtherVectorDx]&kIsDupFlag)!=0) 
        removeLinkFromHeap(theHeapLinkDx);
      else {
        float theDistance2=mDistance2ss[0][theHeapLinkDx];
        theLinkVectorDxs[theNLinks]=theOtherVectorDx;
        theLinkDistance2s[theNLinks]=theDistance2;
        theNLinks++;
      }
    }
    
    // The link arrays are the wrong size if there was a dup - should almost never happen
    if (theNLinks<theLinkVectorDxs.length) {
      int[] theOldVectorDxs=theLinkVectorDxs;
      float[] theOldDistance2s=theLinkDistance2s;
      theLinkVectorDxs=new int[theNLinks];
      theLinkDistance2s=new float[theNLinks];
      System.arraycopy(theOldVectorDxs,0,theLinkVectorDxs,0,theNLinks);
      System.arraycopy(theOldDistance2s,0,theLinkDistance2s,0,theNLinks);
    }
    
    if (inFar) {
      mFarLinkVectorDxs=theLinkVectorDxs;
      mFarLinkDistance2s=theLinkDistance2s;
    } else {
      mNearLinkVectorDxs=theLinkVectorDxs;
      mNearLinkDistance2s=theLinkDistance2s;
      // This is the last step in finding links so can free memory
      mOtherVectorDxss=null;
      mDistance2ss=null;
      mHeapLinkDxs=null;
      mLinkHeapDxs=null;
    }
    gNLinksKept+=theNLinks;
    
  }

//--------------------------------------------------------------------------------------------------------
// createLinks single threaded
//--------------------------------------------------------------------------------------------------------
/*
  public void createLinks2(NeighborSet inNeighborSet) {
    
    // Before creating links for this new node, store existing links from heap
    // These will be the long links used by the descend phase of search
    keepLinks(true);
        
    // Start looking for dups
    mDupVectorDx1=kNotFound;
    mDupVectorDx2=kNotFound;

    int theNVectors=gDataSet.getNVectors();
    int theNNeighbors;
    boolean theTakeShortcut=(gAvgNearDistance2>gShortcutDistance2);
    if (theTakeShortcut) 
      theNNeighbors=theNVectors;
    else {
      inNeighborSet.clearAll();
      collect2ndNeighbors(inNeighborSet);
      theNNeighbors=inNeighborSet.getNNeighbors();
    }
    
    float[] theCreateVector=gDataSet.getVector(mVectorDx);
    
    // Loop over neighbors
    for (int i=0; i<theNNeighbors; i++) {
    
      // Don't waste effort calculating distances to neighbor that are already nodes - already calculated when node created
      int theNeighborVectorDx=(theTakeShortcut?i:inNeighborSet.getVectorDx(i));
      if ((gVectorFlags[theNeighborVectorDx]&kIsNodeFlag)==0) {
  
        // Calc distance to neighbor
        float[] theNeighborVector=gDataSet.getVector(theNeighborVectorDx);
        float theNeighborDistance2=(float) VectorUtils.vectorSeparation2(theCreateVector,theNeighborVector);
        gNCalcs++;
      
        // Check if we have uncovered a dup
        if (theNeighborDistance2==0) {
          handleDup(theNeighborVectorDx,mVectorDx);
          gNUsefulCalcs++;

        // Not a dup
        } else {
          
          // Both endpts have different limits for what they consider near
          boolean theNeighborIsNear=(theNeighborDistance2<gNearDistance2s[mVectorDx]);
          boolean theIsNearNeighbor=(theNeighborDistance2<gNearDistance2s[theNeighborVectorDx]);

          // Only create link if one or both of the endpts is near the other
          if ((theNeighborIsNear)||(theIsNearNeighbor)) {
            gNUsefulCalcs++;

            // Update create heap for neighbor
            CreateHeap.updateMinDistance2(theNeighborVectorDx,theNeighborDistance2);

            // If neighbor is not near vector, add link vector's list
            if (!theNeighborIsNear) 
              addLinkToList(theNeighborVectorDx,theNeighborDistance2);
            // else, add link to vector's heap
            else 
              addLinkToHeap(theNeighborVectorDx,theNeighborDistance2);
            
            // If link not near neighbor, add link to neighbor's list
            IndexVector theOtherVector=gIndexVectors[theNeighborVectorDx];               // R/W vec
            if (!theIsNearNeighbor) 
              theOtherVector.addLinkToList(mVectorDx,theNeighborDistance2);            
            // else, add link to neighbor's heap
            else 
              theOtherVector.addLinkToHeap(mVectorDx,theNeighborDistance2);
          }
        } 
      } 
    }

    // If any dups found during createLink calls, handle them now
    if (mDupVectorDx1!=kNotFound) {
      if (((gVectorFlags[mDupVectorDx1]&kIsDupFlag)!=0)||((gVectorFlags[mDupVectorDx2]&kIsDupFlag)!=0))
        throw new RuntimeException("Found dup that was already found");
      else if ((gVectorFlags[mDupVectorDx1]&kIsNodeFlag)==0)
        handleDup(mDupVectorDx1,mDupVectorDx2);
      else if ((gVectorFlags[mDupVectorDx2]&kIsNodeFlag)==0)
        handleDup(mDupVectorDx2,mDupVectorDx1);
    }
  }
*/
  
//--------------------------------------------------------------------------------------------------------
// DistanceJob 
//
// Provides multi-threading support for calculating chunks of distances simultaneously
//--------------------------------------------------------------------------------------------------------

  private static final int                 kChunkSize=3072;  // Smaller is better for larger D and/or lots of cores
  private static final Vector              kDistanceJobPool=new Vector();
  
  public static class DistanceJob {
    
    private static final int               kNDistanceThreads=(int) Math.round(0.8*kNCores);
    
    private static final ExecutorService   kDistanceService=
        Executors.newFixedThreadPool(
            kNDistanceThreads,
            new ThreadFactory() {          // Added factory to make runner threads deamons
                public Thread newThread(Runnable r) {
                    Thread theThread=Executors.defaultThreadFactory().newThread(r);
                    theThread.setDaemon(true);
                    return theThread;
                }
            });
     
    // Runnable task that does all the work
    private Runnable      mDistanceTask;
    
    // Lock to wait on until job is done
    private Future        mWaitState;
    
    // Inputs passed into submit
    private int           mCreateVectorDx;
    private NeighborSet   mNeighborSet;
    private int           mChunkDx;

    // Outputs available after waitTillDone completes
    private int           mNCalcs;
    private int           mNUsefulNeighbors;
    private int[]         mNeighborVectorDxs;
    private float[]       mNeighborDistance2s;
      
    // Constructor creates runnable task and allocates space for outputs
    public DistanceJob() {
      mDistanceTask=createDistanceTask();
      mNeighborVectorDxs=new int[kChunkSize];
      mNeighborDistance2s=new float[kChunkSize];
    }

    // Get routines for retrieving outputs after waitTillDone completes
    public int getNCalcs() { return mNCalcs; }
    public int getNUsefulNeighbors() { return mNUsefulNeighbors; }
    public int getNeighborVectorDx(int inNeighborDx) { return mNeighborVectorDxs[inNeighborDx]; }
    public float getNeighborDistance2(int inNeighborDx) { return mNeighborDistance2s[inNeighborDx]; }

    // submit takes inputs and submits runnable task to kDistanceService
    // If NeighborSet is null, task uses all vectors
    public void submit(int inCreateVectorDx, NeighborSet inNeighborSet, int inChunkDx) {
      
      // Hold on to inputs
      mCreateVectorDx=inCreateVectorDx;
      mNeighborSet=inNeighborSet;       // If NeighborSet is null, task uses all vectors
      mChunkDx=inChunkDx;

      // Submit runnable task to kDistanceService
      mWaitState=kDistanceService.submit(mDistanceTask);
    }
      
    // waitTillDone will not return till job is done or dies
    public void waitTillDone() {
      try {
        mWaitState.get(30,TimeUnit.MINUTES);
        mWaitState=null;
      } catch (Exception e) {
        throw new RuntimeException("DistanceJob died",e);
      }
    }
    
    // Create the runnable task that does all the work
    private Runnable createDistanceTask() {
      return new Runnable() {
        public void run() {
    
          mNCalcs=0;
          mNUsefulNeighbors=0;
    
          float[] theCreateVector=gDataSet.getVector(mCreateVectorDx);
    
          // Shortcut is to calc all vectorDxs - no need for a NeighborSet when everything is a neighbor
          boolean theTakeShortcut=(mNeighborSet==null);
          int theNNeighbors=(theTakeShortcut?gDataSet.getNVectors():mNeighborSet.getNNeighbors());
          
          int theStartDx=mChunkDx*kChunkSize;
          int theEndDx=Math.min(theStartDx+kChunkSize,theNNeighbors);
          for (int theDx=theStartDx; theDx<theEndDx; theDx++) {
            int theNeighborVectorDx=(theTakeShortcut?theDx:mNeighborSet.getVectorDx(theDx));
            
            if ((gVectorFlags[theNeighborVectorDx]&kIsNodeFlag)==0) {
              float[] theNeighborVector=gDataSet.getVector(theNeighborVectorDx);
              float theNeighborDistance2=(float) VectorUtils.vectorSeparation2(theCreateVector,theNeighborVector);
              mNCalcs++;
    
              if (theNeighborDistance2==0) {
                mNeighborVectorDxs[mNUsefulNeighbors]=theNeighborVectorDx;
                mNeighborDistance2s[mNUsefulNeighbors]=theNeighborDistance2;
                mNUsefulNeighbors++;
                
              } else {
                
                // Both endpts have different limits for what they consider near
                boolean theNeighborIsNear=(theNeighborDistance2<gNearDistance2s[mCreateVectorDx]);
                boolean theIsNearNeighbor=(theNeighborDistance2<gNearDistance2s[theNeighborVectorDx]);
    
                // Only create link if one or both of the endpts is near the other
                if ((theNeighborIsNear)||(theIsNearNeighbor)) {
                  mNeighborVectorDxs[mNUsefulNeighbors]=theNeighborVectorDx;
                  mNeighborDistance2s[mNUsefulNeighbors]=theNeighborDistance2;
                  mNUsefulNeighbors++;
                }
              }
            }
          }
        }
      };
    }

  };

//--------------------------------------------------------------------------------------------------------
// createLinks multi-threaded
//--------------------------------------------------------------------------------------------------------

  public void createLinks(NeighborSet inNeighborSet) {
    
    if ((gVectorFlags[mVectorDx]&kIsNodeFlag)!=0)
      throw new RuntimeException("Create vector "+mVectorDx+" found to be an existing node");
    gVectorFlags[mVectorDx]|=kIsNodeFlag;

    // Before creating links for this new node, store existing links from heap
    // These will be the long links used by the descend phase of search
    keepLinks(true);

    int theNVectors=gDataSet.getNVectors();
    int theNChunks;

    // Shortcut is to calc all vectorDxs - no need for a NeighborSet when everything is a neighbor
    boolean theTakeShortcut=(gAvgNearDistance2>gShortcutDistance2);
    if (theTakeShortcut) 
      theNChunks=(theNVectors-1)/kChunkSize+1;
    
    // Calc neighborSet
    else {
      inNeighborSet.clearAll();
      collect2ndNeighbors(inNeighborSet);
      
/*
      // Calc MaxNearDistance2 of 2nd neighbors
      // If a 2nd neighbor is further away than MaxNearDistance2, it is impossible for it to link
      // If it is nearer, it will only make a link if it is within its own near distance or the  
      //   create vector's near distance (both of which are likely smaller than the max)
      float theMaxNearDistance2=0;
      int theN2ndNeighbors=inNeighborSet.getNNeighbors();
      for (int i=0; i<theN2ndNeighbors; i++) 
        theMaxNearDistance2=Math.max(theMaxNearDistance2,gNearDistance2s[inNeighborSet.getVectorDx(i)]);
*/      

      
      theNChunks=(inNeighborSet.getNNeighbors()-1)/kChunkSize+1;
    }
        
    DistanceJob[] theActiveJobs=new DistanceJob[theNChunks];
    
    for (int theChunkDx=0; theChunkDx<theNChunks; theChunkDx++) {
      DistanceJob theJob;
      if (kDistanceJobPool.size()==0)
        theJob=new DistanceJob();
      else
        theJob=(DistanceJob) kDistanceJobPool.remove(kDistanceJobPool.size()-1);
      theJob.submit(mVectorDx,(theTakeShortcut?null:inNeighborSet),theChunkDx);
      theActiveJobs[theChunkDx]=theJob;
    }

    // Start looking for dups
    mDupVectorDx1=kNotFound;
    mDupVectorDx2=kNotFound;
    
    // Loop over jobs
    for (int theChunkDx=0; theChunkDx<theNChunks; theChunkDx++) {
      DistanceJob theJob=theActiveJobs[theChunkDx];
      
      // Wait till current job finishes
      // This means later jobs may still be running
      // The only interaction for those jobs with this vector is through reads of gNearDistance2s[] which need not be accurate
      theJob.waitTillDone();
      
      // Loop over neighbors
      int theNUsefulNeighbors=theJob.getNUsefulNeighbors();
      for (int i=0; i<theNUsefulNeighbors; i++) {
        int theNeighborVectorDx=theJob.getNeighborVectorDx(i);
        float theNeighborDistance2=theJob.getNeighborDistance2(i);

        // Check if we have uncovered a dup
        if (theNeighborDistance2==0) 
          handleDup(theNeighborVectorDx,mVectorDx);

        // Not a dup
        else {

          // Both endpts have different limits for what they consider near
          boolean theNeighborIsNear=(theNeighborDistance2<gNearDistance2s[mVectorDx]);
          boolean theIsNearNeighbor=(theNeighborDistance2<gNearDistance2s[theNeighborVectorDx]);

          // Only create link if one or both of the endpts is near the other
          if ((theNeighborIsNear)||(theIsNearNeighbor)) {
            
            // Update minimum distance in create heap for neighbor
            gCreateHeap.updateMinDistance2(theNeighborVectorDx,theNeighborDistance2);
  
            // If neighbor is not near vector, add link to this vector's list
            if (!theNeighborIsNear) 
              addLinkToList(theNeighborVectorDx,theNeighborDistance2);
            // else, add link to this vector's heap
            else 
              addLinkToHeap(theNeighborVectorDx,theNeighborDistance2);
  
            // If link not near neighbor, add link to neighbor's list
            IndexVector theOtherVector=gIndexVectors[theNeighborVectorDx];
            if (!theIsNearNeighbor) 
              theOtherVector.addLinkToList(mVectorDx,theNeighborDistance2);            
            // else, add link to neighbor's heap
            else
              theOtherVector.addLinkToHeap(mVectorDx,theNeighborDistance2);
          }
        }
      }
      
      gNUsefulCalcs+=theNUsefulNeighbors;
      gNCalcs+=theJob.getNCalcs();
      
      // Done with this job's output - return to pool
      kDistanceJobPool.add(theJob);
    }

    // If any dups found during createLink calls, handle them now
    if (mDupVectorDx1!=kNotFound) {
      if (((gVectorFlags[mDupVectorDx1]&kIsDupFlag)!=0)||((gVectorFlags[mDupVectorDx2]&kIsDupFlag)!=0))
        throw new RuntimeException("Found dup that was already found");
      else if ((gVectorFlags[mDupVectorDx1]&kIsNodeFlag)==0)
        handleDup(mDupVectorDx1,mDupVectorDx2);
      else if ((gVectorFlags[mDupVectorDx2]&kIsNodeFlag)==0)
        handleDup(mDupVectorDx2,mDupVectorDx1);
    }
  }

  
  
  
  
//--------------------------------------------------------------------------------------------------------
// The rest of the class is heap and list routines, which are internal implementation details and have private access
//--------------------------------------------------------------------------------------------------------

//--------------------------------------------------------------------------------------------------------
// heapSmaller
//--------------------------------------------------------------------------------------------------------
 
  private boolean heapSmaller(int inHeapDx1, int inHeapDx2) {

    int theLinkDx1=mHeapLinkDxs[inHeapDx1];
    int theLinkDx2=mHeapLinkDxs[inHeapDx2];

    float theDistance21=mDistance2ss[0][theLinkDx1];
    float theDistance22=mDistance2ss[0][theLinkDx2];
    
    if (theDistance21<theDistance22)
      return true;
    else if (theDistance21>theDistance22)
      return false;
    else {
      
      // Get VectorDxs 
      int theVectorDx1=mOtherVectorDxss[0][theLinkDx1];
      int theVectorDx2=mOtherVectorDxss[0][theLinkDx2];
      
      // Dealing with equal distance2s.
      // Rare, so we can spend some time checking for dups
      // Don't bother if already found one
      if (mDupVectorDx1==kNotFound) {
  
        // Check if dup - much faster than calculating (separation==0)
        boolean theVectorsAreDups=VectorUtils.vectorsAreDups(
            gDataSet.getVector(theVectorDx1),gDataSet.getVector(theVectorDx2));
        
        // If dup, keep till end for removal
        if (theVectorsAreDups) {
          mDupVectorDx1=theVectorDx1;
          mDupVectorDx2=theVectorDx2;
        }
      }
      
      // Ties broken by other VectorDx
      return (theVectorDx1<theVectorDx2);
    }
  }

//--------------------------------------------------------------------------------------------------------
// swapHeapDx
//--------------------------------------------------------------------------------------------------------

  private void swapHeapDx(int inHeapDx1, int inHeapDx2) {
    int theLink1Dx=mHeapLinkDxs[inHeapDx1];
    int theLink2Dx=mHeapLinkDxs[inHeapDx2];
    mHeapLinkDxs[inHeapDx1]=theLink2Dx;
    mHeapLinkDxs[inHeapDx2]=theLink1Dx;
    mLinkHeapDxs[theLink1Dx]=inHeapDx2;
    mLinkHeapDxs[theLink2Dx]=inHeapDx1;
  }

//--------------------------------------------------------------------------------------------------------
// fixHeapUp
//--------------------------------------------------------------------------------------------------------

  private void fixHeapUp(int inProblemHeapDx) {
    if (inProblemHeapDx>0) {
      int theParentHeapDx=(inProblemHeapDx-1)/2;
      boolean theProblemBiggerThanParent=!heapSmaller(inProblemHeapDx,theParentHeapDx);
      if (theProblemBiggerThanParent) {
        swapHeapDx(inProblemHeapDx,theParentHeapDx);
        fixHeapUp(theParentHeapDx);
      }
    }
  }

//--------------------------------------------------------------------------------------------------------
// fixHeapDown
//--------------------------------------------------------------------------------------------------------

  private void fixHeapDown(int inProblemHeapDx) {
    // Move biggest to top of heap
    int theLeftHeapDx=2*inProblemHeapDx+1;
    if (theLeftHeapDx<mNInHeap) {
      // Left child present
      boolean theProblemSmallerThanLeft=heapSmaller(inProblemHeapDx,theLeftHeapDx);
      int theRightHeapDx=theLeftHeapDx+1;        
      if (theRightHeapDx>=mNInHeap) {
        // Only left child present
        if (theProblemSmallerThanLeft) {
          // Problem index is Smaller than left - left Biggest
          swapHeapDx(inProblemHeapDx,theLeftHeapDx);
          fixHeapDown(theLeftHeapDx);
        } else {
          // Problem index is Bigger than left - heap already fixed 
        }
      } else {
        // Both children present
        boolean theProblemSmallerThanRight=heapSmaller(inProblemHeapDx,theRightHeapDx);
        if (theProblemSmallerThanLeft) {
          // Problem index is Smaller than left
          if (theProblemSmallerThanRight) {
            // Problem index is Smaller than both
            boolean theLeftSmallerThanRight=heapSmaller(theLeftHeapDx,theRightHeapDx);
            if (theLeftSmallerThanRight) {
              // Right Biggest
              swapHeapDx(inProblemHeapDx,theRightHeapDx);
              fixHeapDown(theRightHeapDx);
            } else {
              // Left Biggest
              swapHeapDx(inProblemHeapDx,theLeftHeapDx);
              fixHeapDown(theLeftHeapDx);
            }
          } else {
            // Problem index is Smaller than left, but Bigger than right - left Biggest
            swapHeapDx(inProblemHeapDx,theLeftHeapDx);
            fixHeapDown(theLeftHeapDx);
          }
        } else {
          // Problem index is Bigger than left
          if (theProblemSmallerThanRight) {
            // Problem index is Bigger than left, but Smaller than right - right Biggest
            swapHeapDx(inProblemHeapDx,theRightHeapDx);
            fixHeapDown(theRightHeapDx);
          } else {
            // Problem index is Bigger than both - heap already fixed 
          }
        }     
      }
    }
  }

//--------------------------------------------------------------------------------------------------------
// addLinkToList
//--------------------------------------------------------------------------------------------------------

  private void addLinkToList(int inOtherVectorDx, float inDistance2) {

    int theAddedLinkDx=mNInList+gIndexNNear;
    int theChunkDx=(theAddedLinkDx>>>8);
    int theInChunkDx=(theAddedLinkDx&0x000000ff);
    
    // If crossed into new chunk
    if (theInChunkDx==0) {
      
      // Ensure can hold enough chunks
      if (theChunkDx==mOtherVectorDxss.length) {
        int theOldNChunks=theChunkDx;
        int theNewNChunks=theChunkDx*2;
        int[][] theOldOtherVectorDxss=mOtherVectorDxss;
        float[][] theOldDistance2ss=mDistance2ss;
        mOtherVectorDxss=new int[theNewNChunks][];
        mDistance2ss=new float[theNewNChunks][];
        System.arraycopy(theOldOtherVectorDxss,0,mOtherVectorDxss,0,theOldNChunks);
        System.arraycopy(theOldDistance2ss,0,mDistance2ss,0,theOldNChunks);
      }
      
      // Allocate new chunk
      if (mOtherVectorDxss[theChunkDx]==null) {
        mOtherVectorDxss[theChunkDx]=new int[kNInChunk];
        mDistance2ss[theChunkDx]=new float[kNInChunk];
      }
    }
    
    // Set link values
    mOtherVectorDxss[theChunkDx][theInChunkDx]=inOtherVectorDx;
    mDistance2ss[theChunkDx][theInChunkDx]=inDistance2;
    
    mNInList++;
  }

//--------------------------------------------------------------------------------------------------------
// addLinkToHeap
//--------------------------------------------------------------------------------------------------------
  
  private void addLinkToHeap(int inOtherVectorDx, float inDistance2) {

    // If heap not full, add new link
    if (mNInHeap<gIndexNNear) {
      
      // Get new LinkDx from free list
      int theAddedLinkDx=mHeadFreeLinkDx;
      mHeadFreeLinkDx=mLinkHeapDxs[theAddedLinkDx];
      
      // Set link values
      mOtherVectorDxss[0][theAddedLinkDx]=inOtherVectorDx;
      mDistance2ss[0][theAddedLinkDx]=inDistance2;
      
      // Grow heap
      int theAddedHeapDx=mNInHeap;
      mNInHeap++;
   
      // Have heap and link pt to each other
      mHeapLinkDxs[theAddedHeapDx]=theAddedLinkDx;
      mLinkHeapDxs[theAddedLinkDx]=theAddedHeapDx;
      
      // Fix heap
      fixHeapUp(theAddedHeapDx);
     
      // If added vector filled up the heap, update near distance
      if (mNInHeap==gIndexNNear) {
        int theTopLinkDx=mHeapLinkDxs[0];
        gNearDistance2s[mVectorDx]=mDistance2ss[0][theTopLinkDx];
      }
      
    // If heap full, replace top link
    } else {

      // Get link values for link at top of heap, which is being replaced
      int theReplacedLinkDx=mHeapLinkDxs[0];
      int theReplacedOtherVectorDx=mOtherVectorDxss[0][theReplacedLinkDx];
      float theReplacedDistance2=mDistance2ss[0][theReplacedLinkDx];
      
      // Set link values
      mOtherVectorDxss[0][theReplacedLinkDx]=inOtherVectorDx;
      mDistance2ss[0][theReplacedLinkDx]=inDistance2;

      // Fix heap
      fixHeapDown(0);
     
      // Update near distance
      int theTopLinkDx=mHeapLinkDxs[0];
      gNearDistance2s[mVectorDx]=mDistance2ss[0][theTopLinkDx];
      
      // Replaced link was pushed out of heap 
      // If replaced link is no longer in its other vector's heap, drop it
      if (inDistance2<=gNearDistance2s[inOtherVectorDx]) 
        addLinkToList(theReplacedOtherVectorDx,theReplacedDistance2);
    }
  }

//--------------------------------------------------------------------------------------------------------
// removeLinkFromList
//--------------------------------------------------------------------------------------------------------

  private void removeLinkFromList(int inRemovedLinkDx) {

    int theTopLinkDx=mNInList+gIndexNNear-1;
    int theTopChunkDx=(theTopLinkDx>>>8);
    int theTopInChunkDx=(theTopLinkDx&0x000000ff);

    if (inRemovedLinkDx!=theTopLinkDx) {
      int theRemovedChunkDx=(inRemovedLinkDx>>>8);
      int theRemovedInChunkDx=(inRemovedLinkDx&0x000000ff);
      int theTopOtherVectorDx=mOtherVectorDxss[theTopChunkDx][theTopInChunkDx];
      float theTopDistance2=mDistance2ss[theTopChunkDx][theTopInChunkDx];
      mOtherVectorDxss[theRemovedChunkDx][theRemovedInChunkDx]=theTopOtherVectorDx;
      mDistance2ss[theRemovedChunkDx][theRemovedInChunkDx]=theTopDistance2;
    }
        
    // If removed last link from chunk, try to drop next chunk, leaving this empty chunk as buffer
    if (theTopInChunkDx==0) {
      int theDropChunk=theTopChunkDx+1;
      if ((mOtherVectorDxss.length>theDropChunk)&&(mOtherVectorDxss[theDropChunk]!=null)) {
        mOtherVectorDxss[theDropChunk]=null;
        mDistance2ss[theDropChunk]=null;
        
        // Occasionally, reduce space for chunks 
        if ((theDropChunk>kMinNChunks)&&(
             (mOtherVectorDxss.length>2*theTopChunkDx)||
             (mOtherVectorDxss.length>theTopChunkDx+256))) {          
          int theNewNChunks=theDropChunk;
          int[][] theOldOtherVectorDxss=mOtherVectorDxss;
          float[][] theOldDistance2ss=mDistance2ss;
          mOtherVectorDxss=new int[theNewNChunks][];
          mDistance2ss=new float[theNewNChunks][];
          System.arraycopy(theOldOtherVectorDxss,0,mOtherVectorDxss,0,theNewNChunks);
          System.arraycopy(theOldDistance2ss,0,mDistance2ss,0,theNewNChunks);
        }
      }      
    }

    // Clear link values
    mOtherVectorDxss[theTopChunkDx][theTopInChunkDx]=kNotFound;
    mDistance2ss[theTopChunkDx][theTopInChunkDx]=kNotFound;

    mNInList--;
  }

//--------------------------------------------------------------------------------------------------------
// removeLinkFromHeap
//--------------------------------------------------------------------------------------------------------

  private void removeLinkFromHeap(int inRemovedLinkDx) {

    // Only dup links are removed from heap
    // All others are pushed out during a replace in addLinkToHeap
    
    int theRemovedHeapDx=mLinkHeapDxs[inRemovedLinkDx];

    // Clear link values
    mOtherVectorDxss[0][inRemovedLinkDx]=kNotFound;
    mDistance2ss[0][inRemovedLinkDx]=kNotFound;

    // Put link on free list
    mLinkHeapDxs[inRemovedLinkDx]=mHeadFreeLinkDx;
    mHeadFreeLinkDx=inRemovedLinkDx;

    mNInHeap--;

    // If removed link was last in heap, we are done
    if (theRemovedHeapDx<mNInHeap) {
      
      // Replace removed link with last link in heap, then repair heap
      swapHeapDx(theRemovedHeapDx,mNInHeap);
      fixHeapUp(theRemovedHeapDx);
      fixHeapDown(theRemovedHeapDx);
      
      // If removed top of heap, update near distance
      if (theRemovedHeapDx==0) {
        int theTopLinkDx=mHeapLinkDxs[0];
        gNearDistance2s[mVectorDx]=mDistance2ss[0][theTopLinkDx];
      }
    }
  }

//--------------------------------------------------------------------------------------------------------
// removeDupLinks
//--------------------------------------------------------------------------------------------------------

  private void removeDupLinks() {
 
    // Loop over links in dup's heap
    for (int i=mNInHeap-1; i>=0; i--) {
      int theHeapLinkDx=mHeapLinkDxs[i];
      removeLinkFromHeap(theHeapLinkDx);
    }
    
    for (int i=mNInList-1; i>=0; i--) {
      int theListLinksDx=gIndexNNear+i;
      removeLinkFromList(theListLinksDx);
    }
    
    // Set dup near distance to zero - nothing will link to it again
    gNearDistance2s[mVectorDx]=0;
  }

//--------------------------------------------------------------------------------------------------------
// collectNearNeighbors
//--------------------------------------------------------------------------------------------------------
  
  private void collectNearNeighbors(NeighborSet inNeighborSet) {
    for (int i=mNInHeap-1; i>=0; i--) {  // Reverse order because link can be removed during loop
      int theHeapLinkDx=mHeapLinkDxs[i];
      int theOtherVectorDx=mOtherVectorDxss[0][theHeapLinkDx];
      if ((gVectorFlags[theOtherVectorDx]&kIsDupFlag)!=0) 
        removeLinkFromHeap(theHeapLinkDx);
      else 
        inNeighborSet.addNeighbor(theOtherVectorDx);        
    }
  }
  
//--------------------------------------------------------------------------------------------------------
// collectNearOtherNeighbors
//--------------------------------------------------------------------------------------------------------
  
  private void collectNearOtherNeighbors(NeighborSet inNeighborSet) {
    for (int i=mNInList-1; i>=0; i--) {  // Reverse order because link can be removed during loop
      int theLinkDx=gIndexNNear+i;
      int theChunkDx=theLinkDx>>>8;
      int theInChunkDx=theLinkDx&0x000000ff;
      int theOtherVectorDx=mOtherVectorDxss[theChunkDx][theInChunkDx];
      float theDistance2=mDistance2ss[theChunkDx][theInChunkDx];
      if ((theDistance2>gNearDistance2s[theOtherVectorDx])||
          ((gVectorFlags[theOtherVectorDx]&kIsDupFlag)!=0))
        removeLinkFromList(theLinkDx);
      else 
        inNeighborSet.addNeighbor(theOtherVectorDx);
    }
  }

//--------------------------------------------------------------------------------------------------------
// collect2ndNeighbors
// 
// Used to collect potential near neighbors.  
// Then their distance is calculated to check if they are actually near
//--------------------------------------------------------------------------------------------------------

  private void collect2ndNeighbors(NeighborSet inNeighborSet) {
    
    // Find 1st neighbors then 2nd

    // Ensure set empty
    inNeighborSet.clearAll();

    // Add self (create vector) to list of neighbors to avoid case of linking out and back to self
    inNeighborSet.addNeighbor(mVectorDx);
    
    // Collect 1st near neighbors (from heap)
    collectNearNeighbors(inNeighborSet);
    int theN1stNearNeighbors=inNeighborSet.getNNeighbors();

    // Collect 1st near other neighbors (from list)
    collectNearOtherNeighbors(inNeighborSet);
    int theN1stNearOtherNeighbors=inNeighborSet.getNNeighbors();
    
    // Loop over all 1st neighbors, both near and near other (heap and list), skipping self at index 0
    for (int j=1; j<theN1stNearOtherNeighbors; j++) {

      // For each 1st neighbor, collect 2nd near neighbors (from heap)
      int the1stNeighborVectorDx=inNeighborSet.getVectorDx(j);
      IndexVector the1stIndexVector=gIndexVectors[the1stNeighborVectorDx]; 

      // This collects both the  Near then Near (Heap then Heap)  AND  
      //                         Near Other then Near (List then Heap)  cases
      the1stIndexVector.collectNearNeighbors(inNeighborSet);  
      
      // This collects the  Near then Near Other (Heap then List)  case
      // The Near Other then Near Other case (List then List)  case has been deliberately skipped
      if (j<theN1stNearNeighbors) 
        the1stIndexVector.collectNearOtherNeighbors(inNeighborSet);  
    }      
    
    // Remove self and 1st neighbors
    inNeighborSet.clear1stNeighbors(theN1stNearOtherNeighbors);
  }

//--------------------------------------------------------------------------------------------------------
// handleDup
//--------------------------------------------------------------------------------------------------------

  private void handleDup(int inDupVectorDx, int inDupOfVectorDx) {

    // If just given (A dup of B), but already have (B dup of C), change (A dup of B) to (A dup of C)
    int theDupOfVectorDx=inDupOfVectorDx;
    if ((gVectorFlags[theDupOfVectorDx]&kIsDupFlag)!=0) {
      // Loop over known dups
      for (int i=0; i<gNDups; i++)
        // Look for B as a dup
        if (gDupVectorDxs[i]==inDupOfVectorDx) {
          theDupOfVectorDx=gDupOfVectorDxs[i];  // Changes A dup of B to A dup of C
          break;
        }
    }
    
    // If just given (B dup of C), but already have (A dup of B), change (A dup of B) to (A dup of C)
    // Loop over known dups
    for (int i=0; i<gNDups; i++)
      // Look for B as a dup of
      if (gDupOfVectorDxs[i]==inDupVectorDx) 
        gDupOfVectorDxs[i]=inDupOfVectorDx;   // Changes A dup of B to A dup of C
    
    if ((gVectorFlags[inDupVectorDx]&kIsNodeFlag)!=0)
      throw new RuntimeException("Existing node "+inDupVectorDx+" found to be a dup");
      
    gDupVectorDxs[gNDups]=inDupVectorDx;
    gDupOfVectorDxs[gNDups]=theDupOfVectorDx;
    gNDups++;
    
    gVectorFlags[inDupVectorDx]|=(kIsNodeFlag|kIsDupFlag);
    
    gIndexVectors[inDupVectorDx].removeDupLinks();
    
    gCreateHeap.removeDupVector(inDupVectorDx);
  }

}




