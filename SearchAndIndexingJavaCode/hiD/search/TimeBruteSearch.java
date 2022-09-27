//--------------------------------------------------------------------------------------------------------
// TimeBruteSearch.java
//--------------------------------------------------------------------------------------------------------

package hiD.search;

import hiD.data.*;
import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// TimeBruteSearch
//--------------------------------------------------------------------------------------------------------

public class TimeBruteSearch extends FormatUtils {

//--------------------------------------------------------------------------------------------------------
// timeBruteSearch
//--------------------------------------------------------------------------------------------------------

  public static double timeBruteSearch(
      DataSet   inDataSet, 
      int       inSearchNNear, 
      boolean   inIncludeDups, 
      DataSet   inQuerySet) throws Exception {

    log("\n\nBrute Search Timing Test");
    log("  Searching in:  "+inDataSet.getStandardFilename());
    log("  Searching for SearchNNear, Ks = "+inSearchNNear+" nearest neighbors");
    log("  "+(inIncludeDups?"Includes":"Does NOT include")+" duplicates");

    int theNQueries=inQuerySet.getNVectors();   
    log("  "+theNQueries+" queries from "+inQuerySet.getStandardFilename());

    // Warmup 
    log("\nBrute Warmup Search"); 
    SearchResultSet theSearchResultSet=BruteSearch.searchSet(
        inDataSet,
        inSearchNNear,
        inIncludeDups,
        inQuerySet);
    
    // Avg of the max of (5 runs or till 2mins passed)
    log("\nBrute Search Timing Stats"); 
    int theNRuns=0;
    double theSum=0;
    long theEndTime=System.currentTimeMillis()+2*60*1000;
    while (true) {
      theSearchResultSet=BruteSearch.searchSet(theSearchResultSet);
      theSum+=theSearchResultSet.getAvgTimePerQuery();
      theNRuns++;
      if ((theNRuns>=100)||((theNRuns>=5)&&(System.currentTimeMillis()>theEndTime)))
        break;
    }
    
    double theAvgTimePerQuery=theSum/theNRuns;
    log("\n\nAverage Time (excluding warmup run):  "+formatDuration(theAvgTimePerQuery)+" per query");
    
    return theAvgTimePerQuery;
  }

//--------------------------------------------------------------------------------------------------------
// run 
//--------------------------------------------------------------------------------------------------------

  public static void run(
      String  inDataSetFilename, 
      String  inQuerySetFilename, 
      String  inSearchNNear, 
      String  inIncludeDups) throws Exception {

    long theStartTime=System.currentTimeMillis();
    log(reportHeader("Time Brute Search",theStartTime));  

    DataSet theDataSet;
    DataSet theQuerySet;
    int theSearchNNear;
    boolean theIncludeDups;

    if (kOnDevBox) {
      theDataSet=DataSet.load("GIST_train_960D_1000Kv");
      theQuerySet=DataSet.load("GIST_test_960D_1000v");
      theSearchNNear=10;
      theIncludeDups=false;

    } else {
      theDataSet=DataSet.load(inDataSetFilename);
      theQuerySet=DataSet.load(inQuerySetFilename);
      theSearchNNear=Integer.parseInt(inSearchNNear);
      theIncludeDups=Boolean.parseBoolean(inIncludeDups);
    }

    timeBruteSearch(theDataSet,theSearchNNear,theIncludeDups,theQuerySet);

    log(reportFooter(theStartTime));
    Thread.sleep(3000);
  }
 
//--------------------------------------------------------------------------------------------------------
// main 
//--------------------------------------------------------------------------------------------------------

  public static void main(String[] inArgs) {
    try {
      String theDataSetFilename=null;
      if (inArgs.length>0)
        theDataSetFilename=inArgs[0];
      String theQuerySetFilename=null;
      if (inArgs.length>1)
        theQuerySetFilename=inArgs[1];
      String theSearchNNear=null;
      if (inArgs.length>2)
        theSearchNNear=inArgs[2];
      String theIncludeDups="false";
      if (inArgs.length>3)
        theIncludeDups=inArgs[3];
      run(theDataSetFilename,theQuerySetFilename,theSearchNNear,theIncludeDups);
    } catch (Throwable e) {
      e.printStackTrace(System.err);
    }
  }
  
}

