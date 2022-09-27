//--------------------------------------------------------------------------------------------------------
// SortUtils
//--------------------------------------------------------------------------------------------------------

package hiD.utils;

import java.util.Random;

//--------------------------------------------------------------------------------------------------------
// SortUtils
//--------------------------------------------------------------------------------------------------------

public class SortUtils implements Constants {

//--------------------------------------------------------------------------------------------------------
// SortUtils consts
//--------------------------------------------------------------------------------------------------------

  public static final int   kLessThan=-1;
  public static final int   kEquals=0;
  public static final int   kGreaterThan=1;

//--------------------------------------------------------------------------------------------------------
// SortUtils class varx
//--------------------------------------------------------------------------------------------------------

  public static Random   theGenerator=new Random();

//--------------------------------------------------------------------------------------------------------
// SortComparers inner classes
//--------------------------------------------------------------------------------------------------------

  //------------------------------------------------------------------------------------------------------
  // Inner class SortComparer
  //
  // Hides everything about what is compared and how it is stored
  //------------------------------------------------------------------------------------------------------

  public abstract static class SortComparer {
    
    private boolean   mDescending;
    
    public SortComparer(boolean inDescending) { mDescending=inDescending; }

    public boolean getDescending() { return mDescending; }

    // Abstract methods implemented by child classes
    public abstract int getLength();
    public abstract int compareIndexes(int inIndex1, int inIndex2);
    public abstract void swapIndexes(int inIndex1, int inIndex2);
    
    // Wrapper for compareIndexes()
    // Handles difference between ascending vs. descending sorts
    public int sortCompare(int inIndex1, int inIndex2) {
      int theDiff;
      if (mDescending)
        theDiff=compareIndexes(inIndex2,inIndex1);
      else 
        theDiff=compareIndexes(inIndex1,inIndex2); 
      return theDiff;
    }
  }

  //------------------------------------------------------------------------------------------------------
  // Inner class IntArraySortComparer
  //------------------------------------------------------------------------------------------------------

  public static class IntArraySortComparer extends SortComparer {

    private int[]  mInts;

    public IntArraySortComparer(int[] inInts, boolean inDescending) { 
      super(inDescending);
      mInts=inInts; 
    }

    public int getLength() { return mInts.length; }
    public int[] getInts() { return mInts; }
    public int getInt(int inIndex) { return mInts[inIndex]; }

    public int compareIndexes(int inIndex1, int inIndex2) { 
      int theDiff=getInt(inIndex1)-getInt(inIndex2); 
      if (theDiff>0)
        return kGreaterThan;
      else if (theDiff<0)
        return kLessThan;
      else
        return kEquals;
    }

    public void swapIndexes(int inIndex1, int inIndex2) {
      int theTemp=mInts[inIndex1];
      mInts[inIndex1]=mInts[inIndex2];
      mInts[inIndex2]=theTemp;
    }
  }

  //------------------------------------------------------------------------------------------------------
  // Inner class FloatArraySortComparer
  //------------------------------------------------------------------------------------------------------

  public static class FloatArraySortComparer extends SortComparer {

    private float[]  mFloats;

    public FloatArraySortComparer(float[] inFloats, boolean inDescending) { 
      super(inDescending);
      mFloats=inFloats; 
    }

    public int getLength() { return mFloats.length; }
    public float[] getFloats() { return mFloats; }
    public float getFloat(int inIndex) { return mFloats[inIndex]; }

    public int compareIndexes(int inIndex1, int inIndex2) { 
      float theDiff=getFloat(inIndex1)-getFloat(inIndex2); 
      if (theDiff>0)
        return kGreaterThan;
      else if (theDiff<0)
        return kLessThan;
      else
        return kEquals;
    }

    public void swapIndexes(int inIndex1, int inIndex2) {
      float theTemp=mFloats[inIndex1];
      mFloats[inIndex1]=mFloats[inIndex2];
      mFloats[inIndex2]=theTemp;
    }
  }

  //------------------------------------------------------------------------------------------------------
  // Inner class StringArraySortComparer
  //------------------------------------------------------------------------------------------------------

  public static class StringArraySortComparer extends SortComparer {

    private String[]  mStrings;

    public StringArraySortComparer(String[] inStrings, boolean inDescending) { 
      super(inDescending);
      mStrings=inStrings; 
    }

    public int getLength() { return mStrings.length; }
    public String[] getStrings() { return mStrings; }
    public String getString(int inIndex) { return mStrings[inIndex]; }

    public int compareIndexes(int inIndex1, int inIndex2) { 
      // Case insensitive comparison
      String theStr1=getString(inIndex1);
      String theStr2=getString(inIndex2);
      String theLoStr1=theStr1.toLowerCase();
      String theLoStr2=theStr2.toLowerCase();
      int theDiff=theLoStr1.compareTo(theLoStr2); 
      if (theDiff>0)
        return kGreaterThan;
      else if (theDiff<0)
        return kLessThan;
      else {
        // Case breaks ties
        theDiff=theStr1.compareTo(theStr2); 
        if (theDiff>0)
          return kGreaterThan;
        else if (theDiff<0)
          return kLessThan;
        else
          return kEquals;
      }
    }

    public void swapIndexes(int inIndex1, int inIndex2) {
      String theTemp=mStrings[inIndex1];
      mStrings[inIndex1]=mStrings[inIndex2];
      mStrings[inIndex2]=theTemp;
    }
  }

  //------------------------------------------------------------------------------------------------------
  // Inner class SortMapComparer
  //------------------------------------------------------------------------------------------------------

  public static class SortMapComparer extends SortComparer {

    private int[]          mSortMap;
    private SortComparer   mComparer;

    public SortMapComparer(SortComparer inComparer, int[] ioSortMap) {
      super(inComparer.getDescending());
      if (ioSortMap.length<inComparer.getLength())
        throw new RuntimeException("Incompatible sortmap length");
      for (int i=0; i<inComparer.getLength(); i++)
        ioSortMap[i]=i;
      mComparer=inComparer;
      mSortMap=ioSortMap;
    }

    public SortMapComparer(SortComparer inComparer) {
      this(inComparer,new int[inComparer.getLength()]); }

    public int getLength() { return mComparer.getLength(); }
    public int[] getSortMap() { return mSortMap; }

    public int compareIndexes(int inIndex1, int inIndex2) {
      return mComparer.compareIndexes(mSortMap[inIndex1],mSortMap[inIndex2]); }

    public void swapIndexes(int inIndex1, int inIndex2) {
      int theTemp=mSortMap[inIndex1];
      mSortMap[inIndex1]=mSortMap[inIndex2];
      mSortMap[inIndex2]=theTemp;
    }
  }

  
  
//--------------------------------------------------------------------------------------------------------
// quickSort
//
// Recursive routine - track depth so can alternate strategies
//--------------------------------------------------------------------------------------------------------

  private static void quickSort(
      int            inFirstIndex, 
      int            inNValues, 
      int            inMinIndex, 
      int            inMaxIndex, 
      SortComparer   inSortComparer, 
      int            inDepth) {             
    
    if (inDepth>100) 
      throw new RuntimeException("Too much recursion in quickSort");
    if (inNValues<0)
      throw new RuntimeException("Bad NValues");
    if (inMaxIndex<inMinIndex)
      throw new RuntimeException("Bad limits");

    // Don't need to sort values that are known to be outside range inMinIndex to inMaxIndex
    int inLastIndex=inFirstIndex+inNValues-1;
    if ((inFirstIndex>inMaxIndex)||(inLastIndex<inMinIndex))
      return;
    
    if (inNValues<=1)
      return;

    if (inNValues==2) {
      if (inSortComparer.sortCompare(inFirstIndex,inFirstIndex+1)>0)
        inSortComparer.swapIndexes(inFirstIndex,inFirstIndex+1);
      return;
    }

    // For small numbers of values, use bubble sort
    if (inNValues<16) {
      for (int j=inLastIndex; j>inFirstIndex; j--)
        for (int i=inFirstIndex; i<j; i++)
          if (inSortComparer.sortCompare(i,i+1)>0)
            inSortComparer.swapIndexes(i,i+1);
      return;
    }
    
    // Pick a pivot value from the middle of the range
    int theQuarter=(inNValues>>>2);
    int theRandomIndex=(int) Math.floor((theQuarter<<1)*theGenerator.nextDouble());
    int thePivotIndex=inFirstIndex+theQuarter+theRandomIndex;
    
    // Can avoid some pathological cases by alternating comparison of >= pivot with <= pivot
    boolean theEven=((inDepth&0x0001)==0);
    if (theEven) {

      // Don't know what kind of value it is so can't hold it in a local var
      // Move pivot to the top (i.e. to inLastIndex) to keep it out of the way
      inSortComparer.swapIndexes(thePivotIndex,inLastIndex);

      // Divide values into two sets 1) those less than the pivot and 2) those greater
      // than or equal to pivot.  Swap elements that don't meet this criteria.  The two sets start at
      // the top and bottom of the range of values and grow inward.  Stop division when they touch.
      int theFirstIndex=inFirstIndex;
      int theLastIndex=inLastIndex;  // Starts at a value >= the pivot value
      while (theFirstIndex<theLastIndex) {

        // Search forward from theFirstIndex until theFirstIndex == theLastIndex
        // or find value >= the pivot value
        while ((theFirstIndex<theLastIndex)&&
            (inSortComparer.sortCompare(theFirstIndex,inLastIndex)<0))
          theFirstIndex++;
        
        // Two cases:
        //   1) Advanced until theFirstIndex == theLastIndex
        //   2) Advanced until found value >= the pivot value and stopped
        // In both cases, theFirstIndex value >= the pivot value and all values below theFirstIndex
        // are < the pivot value

        // Search backward from theLastIndex until theFirstIndex == theLastIndex
        // or next value < the pivot value
        // Note we are checking preceding index before moving into it
        // Value at theLastIndex is always >= the pivot value
        while ((theFirstIndex<theLastIndex)&&
            (inSortComparer.sortCompare(theLastIndex-1,inLastIndex)>=0))
          theLastIndex--;

        // Three cases.  
        //   1) theFirstIndex advanced until theFirstIndex == theLastIndex
        // or theFirstIndex advanced until value >= the pivot value and stopped and
        //   2) theLastIndex descended until theFirstIndex == theLastIndex
        //   3) or theLastIndex descended until next value < the pivot value and stopped

        // For cases 1 & 2, we're done without need to swap and the indexs value >= the pivot value 
        // and all values below the indexs < the pivot value
        
        // For case 3, swap elements at theFirstIndex and theLastIndex-1
        if (theFirstIndex<theLastIndex) {
          inSortComparer.swapIndexes(theFirstIndex,theLastIndex-1);
          theFirstIndex++;  // Still sure all values below theFirstIndex < the pivot value
          theLastIndex--;   // Still sure all values at and above theLastIndex >= the pivot value
        }      
      }
       
      // Pivot smallest value of upper range
      // Put pivot value in the new "center" and sort upper and lower lists
      thePivotIndex=theLastIndex;
      inSortComparer.swapIndexes(thePivotIndex,inLastIndex);

    // Odd case, similar to even, but replaces comparison of >= pivot with <= pivot
    } else {

      int theTempIndex=inFirstIndex;  
      inSortComparer.swapIndexes(thePivotIndex,theTempIndex);
 
      int theLastIndex=inLastIndex; 
      int theFirstIndex=theTempIndex; // Starts at a value <= the pivot value
   
      while (theFirstIndex<theLastIndex) {
  
        while ((theFirstIndex<theLastIndex)&&
            (inSortComparer.sortCompare(theLastIndex,theTempIndex)>0))
          theLastIndex--;
  
        while ((theFirstIndex<theLastIndex)&&
            (inSortComparer.sortCompare(theFirstIndex+1,theTempIndex)<=0))
          theFirstIndex++;
        
        if (theFirstIndex<theLastIndex) {
          inSortComparer.swapIndexes(theFirstIndex+1,theLastIndex);
          theLastIndex--;   // Still sure values above theLastIndex > the pivot value
          theFirstIndex++;  // Still sure all values at and below theFirstIndex <= the pivot value
        }    
      }
      
      // Pivot biggest value of lower range
      // Put pivot value in the new "center" and sort upper and lower lists
      thePivotIndex=theFirstIndex;
      inSortComparer.swapIndexes(thePivotIndex,theTempIndex);
    }

    // When data is mostly a single value, there can be a problem with deep recursion 
    // To avoid, don't sort the range of values around pivot that are equal to the pivot
    // These tests will quick fail with normally distributed data, so they don't cost much
    
    // Raise bottom of top range so top range does not include values equal to pivot value
    int theBottomIndex=thePivotIndex+1; 
    while ((theBottomIndex<=inLastIndex)&&
        (inSortComparer.sortCompare(theBottomIndex,thePivotIndex)==0))
      theBottomIndex++;
    
    // Lower top of bottom range so bottom range does not include values equal to pivot value
    int theTopIndex=thePivotIndex-1;
    while ((theTopIndex>=inFirstIndex)&&
        (inSortComparer.sortCompare(theTopIndex,thePivotIndex)==0))
      theTopIndex--;

    int theNLowerValues=theTopIndex+1-inFirstIndex;
    int theNUpperValues=inLastIndex+1-theBottomIndex;
      
    // Recurse into smaller sorts  
    if (theNLowerValues>1)
      quickSort(inFirstIndex,theNLowerValues,inMinIndex,inMaxIndex,inSortComparer,inDepth+1);
    if (theNUpperValues>1)
      quickSort(theBottomIndex,theNUpperValues,inMinIndex,inMaxIndex,inSortComparer,inDepth+1);        
  }
  
//--------------------------------------------------------------------------------------------------------
// quickSort
//--------------------------------------------------------------------------------------------------------

  public static void quickSort(
      int            inFirstIndex, 
      int            inNValues, 
      SortComparer   inSortComparer) {
    quickSort(inFirstIndex,inNValues,inFirstIndex,inFirstIndex+inNValues,inSortComparer,0); }
  
//--------------------------------------------------------------------------------------------------------
// sort
//--------------------------------------------------------------------------------------------------------

  public static void sort(int inFirstIndex, int inNValues, SortComparer inSortComparer) {
    quickSort(inFirstIndex,inNValues,inSortComparer); }

//--------------------------------------------------------------------------------------------------------
// isSorted
//--------------------------------------------------------------------------------------------------------

  public static boolean isSorted(int inFirstIndex, int inNValues, SortComparer inComparer) {
    int theEndIndex=inFirstIndex+inNValues;
    for (int i=inFirstIndex+1; i<theEndIndex; i++)
      if (inComparer.sortCompare(i-1,i)>0)
        return false;
    return true;
  }

//--------------------------------------------------------------------------------------------------------
// sortMap
//--------------------------------------------------------------------------------------------------------

  public static int[] sortMap(SortComparer inComparator) {
    SortMapComparer theSortMapComparer=new SortMapComparer(inComparator);
    sort(0,inComparator.getLength(),theSortMapComparer);
    return theSortMapComparer.getSortMap();
  }



//--------------------------------------------------------------------------------------------------------
// int[] versions
//--------------------------------------------------------------------------------------------------------

  public static void sort(int[] inValues, int inFirstIndex, int inNValues, boolean inDescending) {
    sort(inFirstIndex,inNValues,new IntArraySortComparer(inValues,inDescending)); }
  
  public static void sort(int[] inValues, boolean inDescending) { 
    sort(inValues,0,inValues.length,inDescending); }

  public static boolean isSorted(int[] inValues, int inFirstIndex, int inNValues, boolean inDescending) {
    return isSorted(inFirstIndex,inNValues,new IntArraySortComparer(inValues,inDescending)); }

  public static boolean isSorted(int[] inValues, boolean inDescending) {
    return isSorted(inValues,0,inValues.length,inDescending); }
  
  public static int[] sortMap(int[] inValues, boolean inDescending) { 
    return sortMap(new IntArraySortComparer(inValues,inDescending)); }

  public static void reorder(int[] inValues, int[] inSortMap) {
    int[] theTempValues=inValues.clone();
    for (int i=0; i<inSortMap.length; i++)
      inValues[i]=theTempValues[inSortMap[i]];
  }


  
//--------------------------------------------------------------------------------------------------------
// float[] versions
//--------------------------------------------------------------------------------------------------------

  public static void sort(float[] inValues, int inFirstIndex, int inNValues, boolean inDescending) {
    sort(inFirstIndex,inNValues,new FloatArraySortComparer(inValues,inDescending)); }
  
  public static void sort(float[] inValues, boolean inDescending) { 
    sort(inValues,0,inValues.length,inDescending); }

  public static boolean isSorted(float[] inValues, int inFirstIndex, int inNValues, boolean inDescending) {
    return isSorted(inFirstIndex,inNValues,new FloatArraySortComparer(inValues,inDescending)); }

  public static boolean isSorted(float[] inValues, boolean inDescending) {
    return isSorted(inValues,0,inValues.length,inDescending); }

  public static int[] sortMap(float[] inValues, boolean inDescending) { 
    return sortMap(new FloatArraySortComparer(inValues,inDescending)); }

  public static void reorder(float[] inValues, int[] inSortMap) {
    float[] theTempValues=inValues.clone();
    for (int i=0; i<inSortMap.length; i++)
      inValues[i]=theTempValues[inSortMap[i]];
  }


  
//--------------------------------------------------------------------------------------------------------
// String[] versions
//--------------------------------------------------------------------------------------------------------

  public static void sort(String[] inValues, int inFirstIndex, int inNValues, boolean inDescending) {
    sort(inFirstIndex,inNValues,new StringArraySortComparer(inValues,inDescending)); }
  
  public static void sort(String[] inValues, boolean inDescending) { 
    sort(inValues,0,inValues.length,inDescending); }

  public static boolean isSorted(String[] inValues, int inFirstIndex, int inNValues, boolean inDescending) {
    return isSorted(inFirstIndex,inNValues,new StringArraySortComparer(inValues,inDescending)); }

  public static boolean isSorted(String[] inValues, boolean inDescending) {
    return isSorted(inValues,0,inValues.length,inDescending); }

  public static int[] sortMap(String[] inValues, boolean inDescending) { 
    return sortMap(new StringArraySortComparer(inValues,inDescending)); }

  public static void reorder(String[] inValues, int[] inSortMap) {
    String[] theTempValues=inValues.clone();
    for (int i=0; i<inSortMap.length; i++)
      inValues[i]=theTempValues[inSortMap[i]];
  }

}