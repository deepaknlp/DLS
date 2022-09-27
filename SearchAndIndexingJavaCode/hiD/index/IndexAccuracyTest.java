//--------------------------------------------------------------------------------------------------------
// IndexAccuracyTest.java
//--------------------------------------------------------------------------------------------------------

package hiD.index;

import hiD.data.*;
import hiD.utils.*;
import hiD.search.*;

//--------------------------------------------------------------------------------------------------------
// IndexAccuracyTest
//--------------------------------------------------------------------------------------------------------

public class IndexAccuracyTest extends FormatUtils {

//--------------------------------------------------------------------------------------------------------
// IndexAccuracyTest consts
//--------------------------------------------------------------------------------------------------------

  public static final int    kMaxNTests=1000;
  public static final int    kNThreads=Math.min(kNCores-2,(int) (kNCores*0.8));

//--------------------------------------------------------------------------------------------------------
// Inner class TestThread
//--------------------------------------------------------------------------------------------------------

  public static class TestThread extends Thread {
    
    // Member vars
    Index          mIndex;
    int            mStartQueryDx;
    int            mEndQueryDx;
    int[]          mNMissings;
    int[][]        mCountss;
    long           mElapsedTime;
    double         mSumDistance2;
    long           mNDistanceCalcs;

    // Constructor to do Index searches
    public TestThread(
        Index     inIndex, 
        int       inStartQueryDx,
        int       inEndQueryDx, 
        int[]     inNMissings, 
        int[][]   inCountss) {
      mIndex=inIndex;
      mStartQueryDx=inStartQueryDx;
      mEndQueryDx=inEndQueryDx;
      mNMissings=inNMissings;
      mCountss=inCountss;
      setDaemon(true);
    }
 
    // Code that does the work
    public void run() {

      int theMaxNMissing=mCountss.length;
      int theMaxNNearest=mCountss[0].length;
      DataSet theDataSet=mIndex.getDataSet();

      BruteSearch theBruteSearch=new BruteSearch(theDataSet,theMaxNNearest*2,false);
      SearchResult theBruteSearchResult=new SearchResult(theDataSet);
      
      // Loop over queries
      for (int i=mStartQueryDx; i<mEndQueryDx; i++) {

        // If dup, move to real vector
        int theQueryNodeDx=i;
        if (mIndex.getIsDup(theQueryNodeDx)) {
          theQueryNodeDx=mIndex.getNearestLinkVectorDx(theQueryNodeDx);
          if (mIndex.getIsDup(theQueryNodeDx)) 
            throw new RuntimeException("Dup of dup");
        }

        // Calc true nearest neighbors by Brute force
        theBruteSearchResult=theBruteSearch.search(
            theQueryNodeDx,
            theDataSet.getVector(theQueryNodeDx),
            theDataSet.getDescriptor(theQueryNodeDx),
            theBruteSearchResult);  
        
        // Get links from index
        float[] theLinkDistance2s=mIndex.getLinkDistance2s(theQueryNodeDx);
        int theNLinks=theLinkDistance2s.length;
        
        int theLinkRank=0;
        int theBruteRank=0;

        while (theLinkDistance2s[theLinkRank]==0)
          theLinkRank++;
        while (theBruteSearchResult.getNearDistance2(theBruteRank)==0)
          theBruteRank++;
        
        int theRowDx=0;
        int theNMissing=0;
        while (true) {
          
          float theLinkDistance2=(theLinkRank>=theNLinks?Float.MAX_VALUE:theLinkDistance2s[theLinkRank++]);  
          float theBruteDistance2=theBruteSearchResult.getNearDistance2(theBruteRank++); 

          while ((theRowDx<theMaxNNearest)&&(theBruteDistance2<theLinkDistance2)) {
            theNMissing++;
            mNMissings[theRowDx]+=theNMissing;
            if (theNMissing<theMaxNMissing)
              mCountss[theNMissing][theRowDx]++;
            theRowDx++;
            if (theRowDx>=theMaxNNearest) 
              break;
            
            theBruteDistance2=theBruteSearchResult.getNearDistance2(theBruteRank++); 
          }
          
          if (theRowDx>=theMaxNNearest) 
            break;
        
          if (theLinkDistance2<theBruteDistance2) 
            throw new RuntimeException("Index search distance less than brute search");
  
          mNMissings[theRowDx]+=theNMissing;
          if (theNMissing<theMaxNMissing)
            mCountss[theNMissing][theRowDx]++;
          theRowDx++;
          if (theRowDx>=theMaxNNearest) 
            break;
        }
      }
    }
  };

//--------------------------------------------------------------------------------------------------------
// testAccuracy
//--------------------------------------------------------------------------------------------------------

  public static final int    kNCountCols=6;
  public static final int    kNExtraRows=20;
 
  public static void testAccuracy(Index inIndex) throws Exception {

    int theIndexNNear=inIndex.getIndexNNear();
    int theNCountRows=Math.max(50,theIndexNNear+kNExtraRows);
    
    log("\n\nIndex Accuracy Test, Multi-threaded");
    log("  Index holds "+theIndexNNear+" nearest neighbors");
    log("  Testing accuracy up to "+theNCountRows+" nearest neighbors");
    log("  Does NOT include duplicates");

    int theNTests=Math.min(kMaxNTests,inIndex.getNVectors());
    log("  "+theNTests+" nodes tested");

    int[][] theNMissingss=new int[kNThreads][theNCountRows];
    int[][][] theCountsss=new int[kNThreads][kNCountCols][theNCountRows];

    TestThread[] theThreads=new TestThread[kNThreads];    
    for (int i=0; i<kNThreads; i++) {
      int theStartQueryDx=(i*theNTests)/kNThreads;
      int theEndQueryDx=((i+1)*theNTests)/kNThreads;
      theThreads[i]=new TestThread(
          inIndex,
          theStartQueryDx,
          theEndQueryDx,
          theNMissingss[i],
          theCountsss[i]);
    }
    
    for (int i=0; i<kNThreads; i++) 
      theThreads[i].start();
    for (int i=0; i<kNThreads; i++) 
      theThreads[i].join();
    
    int[] theNMissings=new int[theNCountRows];
    int[][] theCountss=new int[kNCountCols][theNCountRows];
    for (int i=0; i<kNThreads; i++) {
      for (int k=0; k<theNCountRows; k++)
        theNMissings[k]+=theNMissingss[i][k];
      for (int j=0; j<kNCountCols; j++) 
        for (int k=0; k<theNCountRows; k++)
          theCountss[j][k]+=theCountsss[i][j][k];
    }
    
    log("\nNumber of Missing Nearest Neighbors");
    StringBuffer theBuffer=new StringBuffer(256);
    theBuffer.append("      ");
    for (int j=0; j<kNCountCols; j++) 
      theBuffer.append(leftPad(j,10));
    theBuffer.append("  Expected Missing\n");
    log(theBuffer.toString());
    for (int k=0; k<theNCountRows; k++) {
      theBuffer.setLength(0);
      if (k%10==0)
        theBuffer.append("\n");
      theBuffer.append(leftPad(k+1,6));
      for (int j=0; j<kNCountCols; j++) 
        theBuffer.append(leftPad((theCountss[j][k]==0)?".":formatPercent(theCountss[j][k]/(double) theNTests),10));
      theBuffer.append(leftPad(formatDouble(theNMissings[k]/(double) theNTests,4),12));
      log(theBuffer.toString());
    }
  }
  
//--------------------------------------------------------------------------------------------------------
// run 
//--------------------------------------------------------------------------------------------------------

  public static void run(
      String  inIndexFilename) throws Exception {

    long theStartTime=System.currentTimeMillis();
    log(reportHeader("Index Accuracy Test",theStartTime));  

    if (kOnDevBox) {
      testAccuracy(Index.load("convnext_large_in22k_partial_1536D_1000Kv_20Nr"));
      testAccuracy(Index.load("convnext_large_in22k_partial_1536D_1000Kv_30Nr"));
      testAccuracy(Index.load("convnext_large_in22k_partial_1536D_1000Kv_40Nr"));
      testAccuracy(Index.load("convnext_large_in22k_partial_1536D_1000Kv_50Nr"));
      testAccuracy(Index.load("convnext_large_in22k_partial_1536D_1000Kv_60Nr"));
      
      testAccuracy(Index.load("resnet50_conv5block1_2conv_tf2_partial_512D_1000Kv_30Nr"));
      testAccuracy(Index.load("resnet50_conv5block1_2conv_tf2_partial_512D_1000Kv_40Nr"));
      testAccuracy(Index.load("resnet50_conv5block1_2conv_tf2_partial_512D_1000Kv_60Nr"));
      testAccuracy(Index.load("resnet50_conv5block1_2conv_tf2_partial_512D_1000Kv_80Nr"));
      testAccuracy(Index.load("resnet50_conv5block1_2conv_tf2_partial_512D_1000Kv_110Nr"));

    } else {
      testAccuracy(Index.load(inIndexFilename));
    }

    log(reportFooter(theStartTime));
    Thread.sleep(3000);
  }
 
//--------------------------------------------------------------------------------------------------------
// main 
//--------------------------------------------------------------------------------------------------------

  public static void main(String[] inArgs) {
    try {
      String theIndexFilename=null;
      if (inArgs.length>0)
        theIndexFilename=inArgs[0];
      run(theIndexFilename);
    } catch (Throwable e) {
      e.printStackTrace(System.err);
    }
  }
  
}

