//--------------------------------------------------------------------------------------------------------
// CreateHeap.java
//--------------------------------------------------------------------------------------------------------

package hiD.index;

import hiD.data.*;
import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// CreateHeap
//
// Tracks the minimum distance to all existing nodes for all remaining vectors that have not yet become a node
// This is a big heap!
//
// The vector that has the furthest min distance will become the next node.
// This heap keeps the biggest min distance on top
// The main program will pop the top vector, change it into a node, and calc distances from other 
//   nearby vectors to the new node.  As new distances are calculated, links are added, neighborhoods 
//   around vectors shrink, calls to updateVectorDistance2() check if a vector min distance has changed,  
//   and the heap is updated as if it has
//--------------------------------------------------------------------------------------------------------

public class CreateHeap extends FormatUtils {
   
//--------------------------------------------------------------------------------------------------------
// inner class CreateInfo
//--------------------------------------------------------------------------------------------------------

  public static final class CreateInfo {
    
    private int     mVectorDx;
    private float   mDistance2;

    public void set(int inVectorDx, float inDistance2) {
      mVectorDx=inVectorDx;
      mDistance2=inDistance2;
    }

    public int getVectorDx() { return mVectorDx; }
    public float getDistance2() { return mDistance2; }
  }

//--------------------------------------------------------------------------------------------------------
// CreateHeap member vars
//--------------------------------------------------------------------------------------------------------
  
  private DataSet   mDataSet;

  private int       mNRemaining;
  private int[]     mHeapVectorDxs;
  private int[]     mVectorHeapDxs;
  private float[]   mVectorMinDistance2s;  // Only avail for vectors in heap - reset when vector becomes a node

//--------------------------------------------------------------------------------------------------------
// CreateHeap 
//--------------------------------------------------------------------------------------------------------
  
  public CreateHeap(DataSet inDataSet) {
    
    mDataSet=inDataSet;

    int theNVectors=mDataSet.getNVectors();
    mNRemaining=theNVectors;
    mHeapVectorDxs=new int[theNVectors];
    mVectorHeapDxs=new int[theNVectors];
    mVectorMinDistance2s=new float[theNVectors];

    // Init vectors to a min distance of infinity
    for (int theHeapDx=0; theHeapDx<theNVectors; theHeapDx++) {
      int theVectorDx=theHeapDx;
      mHeapVectorDxs[theHeapDx]=theVectorDx;
      mVectorHeapDxs[theVectorDx]=theHeapDx;
      mVectorMinDistance2s[theVectorDx]=Float.MAX_VALUE;
    }
  }

//--------------------------------------------------------------------------------------------------------
// gets
//--------------------------------------------------------------------------------------------------------
 
  public DataSet getDataSet() { return mDataSet; }

//--------------------------------------------------------------------------------------------------------
// getCreateInfo
//--------------------------------------------------------------------------------------------------------
  
  public void getCreateInfo(CreateInfo outCreateInfo) {

    // Since dups can be removed in several places, hard to predict when done
    // Return zero create distance to indicate indexing complete
    if (mNRemaining==0) 
      outCreateInfo.set(kNotFound,0);
      
    else {        
      
      // Use top of heap - vector with largest min distance 
      int theCreateVectorDx=mHeapVectorDxs[0];
      float theCreateMinDistance2=mVectorMinDistance2s[theCreateVectorDx];
       
      // Copy out vector and distance
      outCreateInfo.set(theCreateVectorDx,theCreateMinDistance2);
       
      // Replace top of heap with last vector in heap, shrink heap, then repair heap
      mNRemaining--;
      swapHeapDx(0,mNRemaining);
      fixHeapDown(0);
  
      // Mark vector as no longer in heap
      mHeapVectorDxs[mNRemaining]=kNotFound;
      mVectorHeapDxs[theCreateVectorDx]=kNotFound;
      mVectorMinDistance2s[theCreateVectorDx]=kNotFound;   
    }      
  }

//--------------------------------------------------------------------------------------------------------
// updateMinDistance2
//--------------------------------------------------------------------------------------------------------

  public void updateMinDistance2(int inVectorDx, float inMinDistance2) {

    // Don't update vectors not in heap
    if (mVectorHeapDxs[inVectorDx]!=kNotFound) 
      
      // Only update if distance small enough to change vector min distance
      if (inMinDistance2<mVectorMinDistance2s[inVectorDx]) {
        
        // Change vector min distance
        mVectorMinDistance2s[inVectorDx]=inMinDistance2;
   
        // Vector is somewhere in the middle of the heap
        int theHeapDx=mVectorHeapDxs[inVectorDx];
        
        // Repair heap
        fixHeapUp(theHeapDx);
        fixHeapDown(theHeapDx);
      }
  }

//--------------------------------------------------------------------------------------------------------
// removeDupVector
//--------------------------------------------------------------------------------------------------------

  public void removeDupVector(int inVectorDx) {

    // Vector is somewhere in the middle of the heap
    int theHeapDx=mVectorHeapDxs[inVectorDx];

    // Replace dup with last vector in heap, shrink heap, then repair heap
    mNRemaining--;
    swapHeapDx(theHeapDx,mNRemaining);
    fixHeapUp(theHeapDx);
    fixHeapDown(theHeapDx);

    // Mark vector as no longer in heap
    mHeapVectorDxs[mNRemaining]=kNotFound;
    mVectorHeapDxs[inVectorDx]=kNotFound;
    mVectorMinDistance2s[inVectorDx]=kNotFound;  
  }


  
  
  
//--------------------------------------------------------------------------------------------------------
// The rest of the class is heap routines, which are internal implementation details and have private access
//--------------------------------------------------------------------------------------------------------

//--------------------------------------------------------------------------------------------------------
// heapSmaller
//--------------------------------------------------------------------------------------------------------

  private boolean heapSmaller(int inHeapDx1, int inHeapDx2) {
    int theVectorDx1=mHeapVectorDxs[inHeapDx1];
    int theVectorDx2=mHeapVectorDxs[inHeapDx2];
    float theMinDistance21=mVectorMinDistance2s[theVectorDx1];
    float theMinDistance22=mVectorMinDistance2s[theVectorDx2];
    if (theMinDistance21<theMinDistance22)
      return true;
    else if (theMinDistance21>theMinDistance22)
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
    int theVectorDx1=mHeapVectorDxs[inHeapDx1];
    int theVectorDx2=mHeapVectorDxs[inHeapDx2];
    mHeapVectorDxs[inHeapDx1]=theVectorDx2;
    mHeapVectorDxs[inHeapDx2]=theVectorDx1;
    mVectorHeapDxs[theVectorDx1]=inHeapDx2;
    mVectorHeapDxs[theVectorDx2]=inHeapDx1;
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
    if (theLeftHeapDx<mNRemaining) {
      // Left child present
      boolean theProblemSmallerThanLeft=heapSmaller(inProblemHeapDx,theLeftHeapDx);  
      int theRightHeapDx=theLeftHeapDx+1;        
      if (theRightHeapDx>=mNRemaining) {      
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

}

