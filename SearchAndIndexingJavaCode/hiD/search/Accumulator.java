//--------------------------------------------------------------------------------------------------------
// Accumulator.java
//--------------------------------------------------------------------------------------------------------

package hiD.search;

import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// Accumulator
//
// The accumulator is a heap that collects the nearest neighbor needed to produce a search result 
//
// CAUTION: distances are shared with the search routine and are read-only by the accumulator
//   This is dangerous, but efficient in RAM and CPU
//   And by moving the heap into this add-on class, the search class becomes simpler
//--------------------------------------------------------------------------------------------------------

public class Accumulator implements Constants {
  
//--------------------------------------------------------------------------------------------------------
// Accumulator member vars
//--------------------------------------------------------------------------------------------------------
  
  private int       mMaxNNear;  
  private int       mSearchNNear;          // Heap starts empty and fills up to MaxNNear

  private int       mNearestVectorDx;      // best of the nearest 
  private int[]     mNearVectorDxs;        // the heap - top of the heap is the worst of the nearest 

  private float[]   mExternalDistance2s;   // Shared access to distances which are calculated and kept in search class

//--------------------------------------------------------------------------------------------------------
// Accumulator 
//--------------------------------------------------------------------------------------------------------
  
  public Accumulator(int inMaxNNear, float[] inExternalDistance2s) {
    mMaxNNear=inMaxNNear;
    mSearchNNear=0;
    mNearestVectorDx=kNotFound;
    mNearVectorDxs=new int[inMaxNNear];
    for (int i=0; i<inMaxNNear; i++)
      mNearVectorDxs[i]=kNotFound;
    mExternalDistance2s=inExternalDistance2s;
  }

//--------------------------------------------------------------------------------------------------------
// gets
//--------------------------------------------------------------------------------------------------------

  public int getMaxNNear() { return mMaxNNear; }
  public int getNNear() { return mSearchNNear; }       // Heap fills up to MaxNNear
  
  public int getNearestVectorDx() { return mNearestVectorDx; }       // Best of the nearest 
  public float getNearestDistance2() { return (mSearchNNear==0)?Float.MAX_VALUE:mExternalDistance2s[mNearestVectorDx]; }

  public int getNearLimitVectorDx() { return (mSearchNNear==0)?kNotFound:mNearVectorDxs[0]; }   // Worst of the nearest 
  public float getNearLimitDistance2() { return (mSearchNNear<mMaxNNear)?Float.MAX_VALUE:mExternalDistance2s[mNearVectorDxs[0]]; }
  
  public int getVectorDx(int inHeapDx) { return mNearVectorDxs[inHeapDx]; }
  public float getDistance2(int inHeapDx) { return mExternalDistance2s[mNearVectorDxs[inHeapDx]]; }
  
//--------------------------------------------------------------------------------------------------------
// copyNear
//
// Non-destructive (accumulator unaffected) and unsorted because heaps are unsorted
// Note: distances don't need to be copied since they are owned by the search class
//--------------------------------------------------------------------------------------------------------

  public int copyNear(int[] outVectorDxs) {
    if (mSearchNNear>outVectorDxs.length) 
      throw new RuntimeException("Mismatch!  Accumulator has too many vectors");
    System.arraycopy(mNearVectorDxs,0,outVectorDxs,0,mSearchNNear);
    return mSearchNNear;
  }
  
//--------------------------------------------------------------------------------------------------------
// removeNear
//
// Destructive (accumulator reset) and sorted by removing vectors from the heap
// Note: distances are not changed here because they are owned by search class
//--------------------------------------------------------------------------------------------------------

  public int removeNear(int[] outVectorDxs) {
    if (mSearchNNear>outVectorDxs.length) 
      throw new RuntimeException("Mismatch!  Accumulator has too many vectors");
    int theNInHeap=mSearchNNear;
    for (int i=0; i<theNInHeap; i++) {
      mSearchNNear--;
      outVectorDxs[mSearchNNear]=mNearVectorDxs[0];
      if (mSearchNNear>0) {
        mNearVectorDxs[0]=mNearVectorDxs[mSearchNNear];
        fixHeapDown(0);
      }
      mNearVectorDxs[mSearchNNear]=kNotFound;
    }
    mNearestVectorDx=kNotFound;
    mSearchNNear=0;
    return theNInHeap;
  }

//--------------------------------------------------------------------------------------------------------
// reset
// Note: does not reset distances - they are owned by search class
//--------------------------------------------------------------------------------------------------------
  
  public void reset() {
    // The accumulator should clean up after itself, but if not, perform a full reset
    if (mSearchNNear!=0) {
      mSearchNNear=0;
      mNearestVectorDx=kNotFound;
      for (int i=0; i<mMaxNNear; i++)
        mNearVectorDxs[i]=kNotFound;
    }
  }

//--------------------------------------------------------------------------------------------------------
// addVectorDx
//
// Note: distances are not changed here because they are owned by search class
// Throws exception if Distance2 is not available in mExternalDistance2s
//
// Returns vectorDx of vector pushed out of the accumulator heap
// If accumulator is not full, returns kNotFound 
// If accumulator is full, 
//   if added vector too far away to be kept, returns kNotFound
//   if added vector near enough, the added vector is kept, the worst near vector is pushed out, 
//     and the worst near vectorDx is returned
//--------------------------------------------------------------------------------------------------------

  public int addVectorDx(int inVectorDx) {    
    
    if (mExternalDistance2s[inVectorDx]==kNotFound)
      throw new RuntimeException("Added vector to accumulator without calculating its distance first");
        
    // If heap not full, add vector to heap
    if (mSearchNNear<mMaxNNear) {

      // If no nearest vectorDx, keep as nearest
      if (mSearchNNear==0)
        mNearestVectorDx=inVectorDx;
      
      // If added vector nearer, update nearest vectorDx
      else {
        float theNearestDistance2=mExternalDistance2s[mNearestVectorDx];
        if (mExternalDistance2s[inVectorDx]<=theNearestDistance2)
        
        // Prev line is quick fail - now refine test
        // To make results from different search strategies easier to compare, remove ambiguity in ordering
        // When distances equal, prefer vector with smaller vectorDx 
        if ((mExternalDistance2s[inVectorDx]<theNearestDistance2)||(inVectorDx<mNearestVectorDx))
          mNearestVectorDx=inVectorDx;
      }

      // Vector put at end of heap
      mNearVectorDxs[mSearchNNear]=inVectorDx;
      
      // Heap fixed
      fixHeapUp(mSearchNNear);
      
      // Number in heap incremented
      mSearchNNear++;
      
      // Return kNotFound
      return kNotFound;

    // Heap full, replace vector in heap
    } else {

      // If added vector too far (distance2 bigger than worst near vector)
      int theLimitVectorDx=mNearVectorDxs[0];
      float theLimitDistance2=mExternalDistance2s[theLimitVectorDx];
      if (mExternalDistance2s[inVectorDx]>=theLimitDistance2) 

        // Prev line is quick fail - now refine test
        // To make results from different search strategies easier to compare, remove ambiguity in ordering
        // When distances equal, prefer vector with smaller vectorDx 
        if ((mExternalDistance2s[inVectorDx]>theLimitDistance2)||(inVectorDx>theLimitVectorDx))

          // Return kNotFound
          return kNotFound;

      // Added vector near enough to include in heap

      // If added vector nearer, update nearest vectorDx
      float theNearestDistance2=mExternalDistance2s[mNearestVectorDx];
      if (mExternalDistance2s[inVectorDx]<=theNearestDistance2)
        
        // Prev line is quick fail - now refine test
        // To make results from different search strategies easier to compare, remove ambiguity in ordering
        // When distances equal, prefer vector with smaller vectorDx 
        if ((mExternalDistance2s[inVectorDx]<theNearestDistance2)||(inVectorDx<mNearestVectorDx))
          mNearestVectorDx=inVectorDx;
      
      // Top of heap is worst near vector and is pushed out of heap
      int theReplacedVectorDx=mNearVectorDxs[0];

      // Vector replaces top of heap
      mNearVectorDxs[0]=inVectorDx;
      
      // Heap fixed
      fixHeapDown(0);
      
      // Return worst vectorDx
      return theReplacedVectorDx;
    }
  }

  
  
//--------------------------------------------------------------------------------------------------------
// The rest of the class is heap routines, which are internal implementation details and have private access
//--------------------------------------------------------------------------------------------------------

//--------------------------------------------------------------------------------------------------------
// heapSmaller
//--------------------------------------------------------------------------------------------------------

  private boolean heapSmaller(int inHeapDx1, int inHeapDx2) {
    int theVectorDx1=mNearVectorDxs[inHeapDx1];
    int theVectorDx2=mNearVectorDxs[inHeapDx2];
    float theDistance21=mExternalDistance2s[theVectorDx1];
    float theDistance22=mExternalDistance2s[theVectorDx2];
    if (theDistance21<theDistance22)
      return true;
    else if (theDistance21>theDistance22)
      return false;
    else 
      // Dealing with equal distance2s - very rare
      // Ties broken by VectorDx
      return (theVectorDx1<theVectorDx2);
  }

//--------------------------------------------------------------------------------------------------------
// swapHeapDx
//--------------------------------------------------------------------------------------------------------

  private void swapHeapDx(int inHeapDx1, int inHeapDx2) {
    int theSwapVectorDx=mNearVectorDxs[inHeapDx1];
    mNearVectorDxs[inHeapDx1]=mNearVectorDxs[inHeapDx2];
    mNearVectorDxs[inHeapDx2]=theSwapVectorDx;
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
    int theLeftHeapDx=2*inProblemHeapDx+1;
    if (theLeftHeapDx<mSearchNNear) {
      // Left child present
      boolean theProblemSmallerThanLeft=heapSmaller(inProblemHeapDx,theLeftHeapDx);
      int theRightHeapDx=theLeftHeapDx+1;        
      if (theRightHeapDx>=mSearchNNear) {      
        // Only left child present
        if (theProblemSmallerThanLeft) {
          // Problem index is Smaller than left - left best
          swapHeapDx(inProblemHeapDx,theLeftHeapDx);
          fixHeapDown(theLeftHeapDx);
        } else {
          // Problem index is Bigger than left - heap fixed 
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
              // Right Bigger
              swapHeapDx(inProblemHeapDx,theRightHeapDx);
              fixHeapDown(theRightHeapDx);
            } else {
              // Left Bigger
              swapHeapDx(inProblemHeapDx,theLeftHeapDx);
              fixHeapDown(theLeftHeapDx);
            }
          } else {
            // Problem index is Smaller than left, but Bigger than right - left Bigger
            swapHeapDx(inProblemHeapDx,theLeftHeapDx);
            fixHeapDown(theLeftHeapDx);
          }
        } else {
          // Problem index is Bigger than left
          if (theProblemSmallerThanRight) {
            // Problem index is Bigger than left, but Smaller than right - right Bigger
            swapHeapDx(inProblemHeapDx,theRightHeapDx);
            fixHeapDown(theRightHeapDx);
          } else {
            // Problem index is Bigger than both - heap fixed 
          }
        }     
      }
    }
  }

}

