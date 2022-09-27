//--------------------------------------------------------------------------------------------------------
// NeighborSet.java
//--------------------------------------------------------------------------------------------------------

package hiD.index;

import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// NeighborSet
//
// Used to collect 2nd neighbors to create new links
// Often accumulate the same neighbor twice
//--------------------------------------------------------------------------------------------------------

public class NeighborSet implements Constants {
  
//--------------------------------------------------------------------------------------------------------
// NeighborSet member vars
//--------------------------------------------------------------------------------------------------------
  
  private int         mNNeighbors;     // Number of vectors in set
  private int[]       mVectorDxs;      // Vector indexes in order added
  private boolean[]   mInUses;         // Flag indicating vector is already in set

//--------------------------------------------------------------------------------------------------------
// NeighborSet 
//--------------------------------------------------------------------------------------------------------
  
  public NeighborSet(int inNVectors) { 
    mNNeighbors=0;
    mVectorDxs=new int[inNVectors];
    mInUses=new boolean[inNVectors];
    for (int i=0; i<inNVectors; i++) {
      mVectorDxs[i]=kNotFound;
      mInUses[i]=false;
    }
  }

//--------------------------------------------------------------------------------------------------------
// gets
//--------------------------------------------------------------------------------------------------------

  public int getNNeighbors() { return mNNeighbors; }
  public int getVectorDx(int inNeighborDx) { return mVectorDxs[inNeighborDx]; }
  public boolean getInUse(int inVectorDx) { return mInUses[inVectorDx]; }
  
//--------------------------------------------------------------------------------------------------------
// addNeighbor
//--------------------------------------------------------------------------------------------------------

  public void addNeighbor(int inVectorDx) { 
    // Using the InUses array to track whether this neighbor is already in the set
    if (!mInUses[inVectorDx]) {
      mInUses[inVectorDx]=true;
      mVectorDxs[mNNeighbors]=inVectorDx;
      mNNeighbors++;
    } 
  }

//--------------------------------------------------------------------------------------------------------
// clearAll
//--------------------------------------------------------------------------------------------------------

  public void clearAll() {
    // Clear InUses and VectorDxs
    for (int i=0; i<mNNeighbors; i++) {
      mInUses[mVectorDxs[i]]=false;
      mVectorDxs[i]=kNotFound;
    }
    // Clear number of neighbors
    mNNeighbors=0;
  }

//--------------------------------------------------------------------------------------------------------
// clear1stNeighbors
//
// For efficiency, this set is used to collect 1st neighbors and the 2nd neighbors
// The 1st neighbors are in the start of the arrays, and can be removed to leave only 2nd neighbors
//--------------------------------------------------------------------------------------------------------

  public void clear1stNeighbors(int inN1stNeighbors) {
    
    // Clear InUses for 1st neighbors
    for (int i=0; i<inN1stNeighbors; i++) 
      mInUses[mVectorDxs[i]]=false;

    int theN2ndNeighbors=mNNeighbors-inN1stNeighbors;

    // Move 2nd neighbor vectorDxs down to overwrite where 1st neighbors used to be
    // Still in order that vectors were added
    System.arraycopy(mVectorDxs,inN1stNeighbors,mVectorDxs,0,theN2ndNeighbors);
    
    // Clear array where 2nd neighbors have been moved away from
    for (int i=theN2ndNeighbors; i<mNNeighbors; i++) 
      mVectorDxs[i]=kNotFound;
    
    // Set number of neighbors to just 2nd
    mNNeighbors=theN2ndNeighbors;
  }  

}








