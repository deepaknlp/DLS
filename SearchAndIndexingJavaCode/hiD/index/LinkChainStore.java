//--------------------------------------------------------------------------------------------------------
// LinkChainStore.java
//
// A chain store accumulates link triplets (vector1Dx, vector2Dx, distance2) in chains of 
//   pairs (vector2Dx, distance2) for each vector1Dx
// Links are added in a random order, and then all pairs for each vector1Dx are read out at the end.
// The chains are stored as linked lists, and each pair has a pointer to the previous pair
//
// Java arrays use int indexing, and are limited to a max of 2^31-1 elements.
// To handle all links for all vectors, we need something bigger than a java array
// So we use long indexing mapped to a 2D array.
// Not really interesting enough to explain, but that's why the member vars are 2D arrays and the
//   code talks about offsets and slices
//
// Usage:
//  1) Append links one at a time as triplets {Vector1Dx, Vector2Dx, Distance2}
//  2) After all links have been appended, read out all the links for each Vector1Dx
//  3) To save space, output links are returned as the pairs {Vector2Dx, Distance2} and the Vector1Dx is implied
//--------------------------------------------------------------------------------------------------------

package hiD.index;

import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// LinkChainStore
//--------------------------------------------------------------------------------------------------------

public class LinkChainStore extends FormatUtils {

//--------------------------------------------------------------------------------------------------------
// LinkChainStore consts
//--------------------------------------------------------------------------------------------------------
  
  public static final int   kSliceBits=16; 
  public static final int   kSliceBitMask=~(-1<<kSliceBits);   // 0x0000FFFF
  public static final int   kSliceSize=(1<<kSliceBits);        // 64k

//--------------------------------------------------------------------------------------------------------
// LinkChainStore vars
//--------------------------------------------------------------------------------------------------------

  private long        mNLinks;
  private int[][]     mVectorDxs; 
  private float[][]   mDistance2s; 
  private long[][]    mPrevPtrs; 
  private long[]      mLastPtrs;
  private int[]       mNLinksInChains;

//--------------------------------------------------------------------------------------------------------
// LinkChainStore 
//--------------------------------------------------------------------------------------------------------

  public LinkChainStore(int inNVectors, long inTotNLinks) {
    mNLinks=0;
    int theNSlices=getNRequiredSlices(inTotNLinks);
    mVectorDxs=new int[theNSlices][];
    mDistance2s=new float[theNSlices][];
    mPrevPtrs=new long[theNSlices][];
    for (int i=0; i<theNSlices; i++) {
      mVectorDxs[i]=new int[kSliceSize];      
      mDistance2s[i]=new float[kSliceSize];      
      mPrevPtrs[i]=new long[kSliceSize];      
    }
    mLastPtrs=new long[inNVectors];
    mNLinksInChains=new int[inNVectors];
    for (int i=0; i<inNVectors; i++) {
      mLastPtrs[i]=kNotFound;
      mNLinksInChains[i]=0;
    }
  }

//--------------------------------------------------------------------------------------------------------
// gets
//--------------------------------------------------------------------------------------------------------

  public int getNLinksInChain(int inChainDx) { return mNLinksInChains[inChainDx]; }
  
  private static int getNRequiredSlices(long inCapacity) { return (int) ((inCapacity-1)/kSliceSize+1); }
  private static int getSliceN(long inOffset) { return (int) (inOffset>>>kSliceBits); }
  private static int getSliceOffset(long inOffset) { return ((int) inOffset)&kSliceBitMask; }

//--------------------------------------------------------------------------------------------------------
// getLinkInfos
//--------------------------------------------------------------------------------------------------------

  public int getLinkInfos(int inVector1Dx, int[] ioVector2Dxs, float[] ioDistance2s) {
    int theNInfos=mNLinksInChains[inVector1Dx];
    if (theNInfos>0) {
      long thePtr=mLastPtrs[inVector1Dx];
      int theInfoDx=theNInfos-1;
      while (thePtr!=kNotFound) {
        int theSliceN=getSliceN(thePtr);
        int theSliceOffest=getSliceOffset(thePtr);
        ioVector2Dxs[theInfoDx]=mVectorDxs[theSliceN][theSliceOffest];
        ioDistance2s[theInfoDx]=mDistance2s[theSliceN][theSliceOffest];
        thePtr=mPrevPtrs[theSliceN][theSliceOffest];
        theInfoDx--;
      }
    }
    return theNInfos;
  }

//--------------------------------------------------------------------------------------------------------
// appendLinkInfo
//--------------------------------------------------------------------------------------------------------

  public void appendLinkInfo(int inVector1Dx, int inVector2Dx, float inDistance2) {
    int theSliceN=getSliceN(mNLinks);
    int theSliceOffset=getSliceOffset(mNLinks);
    mVectorDxs[theSliceN][theSliceOffset]=inVector2Dx; 
    mDistance2s[theSliceN][theSliceOffset]=inDistance2; 
    mPrevPtrs[theSliceN][theSliceOffset]=mLastPtrs[inVector1Dx];
    mLastPtrs[inVector1Dx]=mNLinks;
    mNLinksInChains[inVector1Dx]++;
    mNLinks++;
  }

}

