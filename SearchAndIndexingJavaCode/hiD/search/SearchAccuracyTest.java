//--------------------------------------------------------------------------------------------------------
// SearchAccuracyTest.java
//--------------------------------------------------------------------------------------------------------

package hiD.search;

import hiD.data.*;
import hiD.utils.*;
import hiD.index.*;

//--------------------------------------------------------------------------------------------------------
// SearchAccuracyTest
//--------------------------------------------------------------------------------------------------------

public class SearchAccuracyTest extends FormatUtils {

//--------------------------------------------------------------------------------------------------------
// testAccuracy
//--------------------------------------------------------------------------------------------------------

  public static final int    kNCountCols=6;
  public static final int    kNExtraRows=10;

  public static double[] testAccuracy(
      Index             inIndex, 
      int               inSearchNNear, 
      boolean           inIncludeDups,
      DataSet           inQuerySet,
      SearchResultSet   inBruteResultSet) throws Exception {

    System.gc();

    log("\n\nSearching "+inIndex.getStandardFilename()+" for SearchNNear, Ks = "+inSearchNNear+"NN");
    
    SearchResultSet theIndexResultSet=IndexSearch.searchSet(
        inIndex,
        inSearchNNear,
        inIncludeDups,
        inQuerySet);

    int theNCountRows=inSearchNNear+kNExtraRows;
    double[] theExpectedNMissings=new double[theNCountRows];
    double[][] theExpectedFractionss=new double[kNCountCols][theNCountRows];

    // Loop over queries
    int theNQueries=inQuerySet.getNVectors();
    for (int i=0; i<theNQueries; i++) {

      SearchResult theBruteSearchResult=inBruteResultSet.getSearchResult(i);
      SearchResult theIndexSearchResult=theIndexResultSet.getSearchResult(i);

      int theIndexNNear=theIndexSearchResult.getSearchNNear();

      int theIndexRank=0;
      int theBruteRank=0;
      
      int theRowDx=0;
      int theNMissing=0;
      while (true) {
        
        float theIndexDistance2=(theIndexRank>=theIndexNNear?Float.MAX_VALUE:theIndexSearchResult.getNearDistance2(theIndexRank++));   
        float theBruteDistance2=theBruteSearchResult.getNearDistance2(theBruteRank++); 

        while ((theRowDx<theNCountRows)&&(theBruteDistance2<theIndexDistance2)) {
          theNMissing++;
          theExpectedNMissings[theRowDx]+=theNMissing;
          if (theNMissing<kNCountCols)
            theExpectedFractionss[theNMissing][theRowDx]++;
          theRowDx++;
          if (theRowDx>=theNCountRows) 
            break;

          theBruteDistance2=theBruteSearchResult.getNearDistance2(theBruteRank++); 
        }
        
        if (theRowDx>=theNCountRows) 
          break;
      
        if (theIndexDistance2<theBruteDistance2) 
          throw new RuntimeException("Index search distance less than brute search");

        theExpectedNMissings[theRowDx]+=theNMissing;
        if (theNMissing<kNCountCols)
          theExpectedFractionss[theNMissing][theRowDx]++;
        theRowDx++;
        if (theRowDx>=theNCountRows) 
          break;
      }
    }

    for (int k=0; k<theNCountRows; k++)
      theExpectedNMissings[k]/=theNQueries;
    for (int j=0; j<kNCountCols; j++) 
      for (int k=0; k<theNCountRows; k++)
        theExpectedFractionss[j][k]/=theNQueries;
   
    log("\n                    Number of Missing Nearest Neighbors               Expected");
    StringBuffer theBuffer=new StringBuffer(256);
    theBuffer.append("      ");
    for (int j=0; j<kNCountCols; j++) 
      theBuffer.append(leftPad(j,10));
    theBuffer.append("     Missing");
    log(theBuffer.toString());
    for (int k=0; k<theNCountRows; k++) {
      theBuffer.setLength(0);
      if (k%10==0)
        theBuffer.append("\n");
      theBuffer.append(leftPad(k+1,6));
      for (int j=0; j<kNCountCols; j++) 
        if (theExpectedFractionss[j][k]==0)
          theBuffer.append(leftPad(".",10));
        else
          theBuffer.append(leftPad(formatPercent(theExpectedFractionss[j][k]),10));
      theBuffer.append(leftPad(formatDouble(theExpectedNMissings[k],4),12));
      log(theBuffer.toString());
    }
    
    return theExpectedNMissings;
  }

//--------------------------------------------------------------------------------------------------------
// findSearchNNearForRecallAtN
//--------------------------------------------------------------------------------------------------------

  public static int findSearchNNearForRecallAtN(
      Index             inIndex, 
      DataSet           inQuerySet, 
      boolean           inIncludeDups,
      double            inRecall,
      int               inRecallN,
      SearchResultSet   inBruteResultSet) throws Exception {
    
    if ((inRecall<0.9)||(inRecall>=1.0))
      throw new RuntimeException("Recall must be be 0.9 or greater and less than 1.0");
    double inMissingLimit=inRecallN*(1.0-inRecall);
    
    log("\n\nIndex Search Accuracy Test");
    log("  Desire recall of "+formatPercent(inRecall)+" at "+inRecallN+"NN"+
        " - Requires ≤"+formatDouble(inMissingLimit)+" missing at "+inRecallN+"NN");
    log("  Index holds IndexNNear, Ki = "+inIndex.getIndexNNear()+"NN");
    log("  "+(inIncludeDups?"Includes":"Does NOT include")+" duplicates");

    int theNQueries=inQuerySet.getNVectors();
    log("  "+theNQueries+" queries from "+inQuerySet.getStandardFilename());
        
    int[] theSearchNNears=new int[51];   // RecallN+0 through RecallN+250 by steps of 5
    for (int i=0; i<theSearchNNears.length; i++)
      theSearchNNears[i]=inRecallN+i*5;
    double[] theNMissingAtNs=new double[theSearchNNears.length];
    double[] theRecallAtNs=new double[theSearchNNears.length];
    
    int theBestSearchNNear=kNotFound;
    for (int i=0; i<theSearchNNears.length; i++) {
      int theSearchNNear=theSearchNNears[i];
      double[] theExpectedNMissings=testAccuracy(inIndex,theSearchNNear,inIncludeDups,inQuerySet,inBruteResultSet);
      theNMissingAtNs[i]=theExpectedNMissings[inRecallN-1];
      theRecallAtNs[i]=1.0-theNMissingAtNs[i]/inRecallN;
      log("\n  Recall of "+formatDouble(theRecallAtNs[i])+" at "+inRecallN+
          "NN with a search for Ks = "+theSearchNNear+"NN");
      if (theExpectedNMissings[inRecallN-1]<inMissingLimit) {
        if (theBestSearchNNear==kNotFound) {
          theBestSearchNNear=theSearchNNear;
          log("    *** Best SearchNNear - accurate enough and fast as possible");
        } else 
          log("    Slower than necessary");
      } else
        log("    Not accurate enough");
      // Normally stop at +50, but continue up to max to find BestSearchNNear
      if ((theBestSearchNNear!=kNotFound)&&(i>=10))
        break;
    }
    
    log("\n"+kDivider);
    log("\nSearch accuracy for "+inIndex.getStandardFilename()+" at "+inRecallN+
        "NN with IndexNNear, Ki = "+inIndex.getIndexNNear()+"NN, and various SearchNNears, Ks");
    log("\n          Ks     Missing      Recall");
    for (int i=0; i<theSearchNNears.length; i++) {
      log(leftPad(theSearchNNears[i],12)+
          leftPad(theNMissingAtNs[i],12)+
          leftPad(formatPercent(theRecallAtNs[i]),12)+
          (theBestSearchNNear==theSearchNNears[i]?"    <--- Best so far":""));
      // Normally show up to +50, but extend table to show BestSearchNNear when necessary
      if ((theBestSearchNNear!=kNotFound)&&(i>=10)&&
          (theSearchNNears[i]>=theBestSearchNNear))
        break;
    }
    log("\n"+kDivider);

    if (theBestSearchNNear==kNotFound) {
      log("\nTest Failed");
      log("Could not achieve recall of "+formatDouble(inRecall)+" at "+inRecallN+"NN for any SearchNNears, Ks");

    } else {
      log("\nAbove scanned for best SearchNNear with steps of 5");
      log("Now refining SearchNNear to absolute best with a few steps of 1");
           
      for (int i=0; i<4; i++) {
        int theSearchNNear=theBestSearchNNear-1;
        double[] theExpectedNMissings=testAccuracy(inIndex,theSearchNNear,inIncludeDups,inQuerySet,inBruteResultSet);
        double theRecallAtN=1.0-theExpectedNMissings[inRecallN-1]/inRecallN;
        log("\n  Recall of "+formatDouble(theRecallAtN)+" at "+inRecallN+"NN with a search for "+theSearchNNear+"NN");
        if (theExpectedNMissings[inRecallN-1]<inMissingLimit) {
          theBestSearchNNear=theSearchNNear;
          log("    *** Better SearchNNear - still accurate enough and faster");
        } else {
          log("    Not accurate enough - done with refinement");
          break;
        }
      }
      
      log("\nAchieved "+formatPercent(inRecall)+" at "+inRecallN+"NN with SearchNNear, Ks = "+theBestSearchNNear+"NN");
      log("  Smaller SearchNNear will have worse recall.  Larger will be slower");
    }
    
    return theBestSearchNNear;
  }

  
  
  public static int findSearchNNearForRecallAtN(
      Index     inIndex, 
      DataSet   inQuerySet, 
      boolean   inIncludeDups,
      double    inRecall,
      int       inRecallN) throws Exception {

    log("\n\nFirst, perform brute force search to get true nearest neighbors");
    log("  Can take a while");

    int theBruteSearchNNear=Math.min(inIndex.getNVectors(),inRecallN+200);
    SearchResultSet theBruteResultSet=BruteSearch.searchSet(
        inIndex.getDataSet(),
        theBruteSearchNNear,
        inIncludeDups,
        inQuerySet);
    
    return findSearchNNearForRecallAtN(
        inIndex,
        inQuerySet, 
        inIncludeDups,
        inRecall,
        inRecallN,
        theBruteResultSet);
  }

//--------------------------------------------------------------------------------------------------------
// run 
//--------------------------------------------------------------------------------------------------------

  public static void run(
      String  inIndexFilename, 
      String  inQuerySetFilename, 
      String  inIncludeDups,
      String  inReall, 
      String  inRecallN) throws Exception {

    long theStartTime=System.currentTimeMillis();
    log(reportHeader("Search Accuracy Test",theStartTime));  

    Index theIndex;
    DataSet theQuerySet;
    boolean theIncludeDups;
    double theRecall;
    int theRecallN;

    if (kOnDevBox) {
//      theIndex=Index.load("convnext_large_in22k_partial_1536D_1000Kv_50Nr");
//      theQuerySet=DataSet.load("convnext_large_in22k_test_1536D_10Kv");
      
//      theIndex=Index.load("resnet50_conv5block1_2conv_tf2_partial_512D_1000Kv_60Nr");
//      theQuerySet=DataSet.load("resnet50_conv5block1_2conv_tf2_test_512D_10Kv");
      
      theIndex=Index.load("GIST_train_960D_1000Kv_50Nr");
      theQuerySet=DataSet.load("GIST_test_960D_1000v");
      
      theIncludeDups=true;
      theRecall=0.99;
      theRecallN=10;
      
    } else {
      theIndex=Index.load(inIndexFilename);
      theQuerySet=DataSet.load(inQuerySetFilename);
      theIncludeDups=Boolean.parseBoolean(inIncludeDups);
      theRecall=Double.parseDouble(inRecallN);
      theRecallN=Integer.parseInt(inRecallN);
    }

    findSearchNNearForRecallAtN(theIndex,theQuerySet,theIncludeDups,theRecall,theRecallN);

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
      String theQuerySetFilename=null;
      if (inArgs.length>1)
        theQuerySetFilename=inArgs[1];
      String theIncludeDups="false";
      if (inArgs.length>2)
        theIncludeDups=inArgs[2];
      String theRecall=null;
      if (inArgs.length>3)
        theRecall=inArgs[3];
      String theRecallN=null;
      if (inArgs.length>4)
        theRecallN=inArgs[4];
      run(theIndexFilename,theQuerySetFilename,theIncludeDups,theRecall,theRecallN);
    } catch (Throwable e) {
      e.printStackTrace(System.err);
      try { Thread.sleep(3000); } catch (Exception e2) { }
    }
  }
  
}

