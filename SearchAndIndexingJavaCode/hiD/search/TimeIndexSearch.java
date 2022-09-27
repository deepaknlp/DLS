//--------------------------------------------------------------------------------------------------------
// TimeIndexSearch.java
//--------------------------------------------------------------------------------------------------------

package hiD.search;

import hiD.data.*;
import hiD.utils.*;
import hiD.index.*;

//--------------------------------------------------------------------------------------------------------
// TimeIndexSearch
//--------------------------------------------------------------------------------------------------------

public class TimeIndexSearch extends FormatUtils {

//--------------------------------------------------------------------------------------------------------
// timeIndexSearch
//--------------------------------------------------------------------------------------------------------

  public static double timeIndexSearch(
      Index     inIndex, 
      int       inSearchNNear, 
      boolean   inIncludeDups, 
      DataSet   inQuerySet) throws Exception {
    
    log("\n\nIndex Search Timing Test");
    log("  Using index:  "+inIndex.getStandardFilename());
    log("  Index holds IndexNNear, Ki = "+inIndex.getIndexNNear()+" nearest neighbors");
    log("  Searching for SearchNNear, Ks = "+inSearchNNear+" nearest neighbors");
    log("  "+(inIncludeDups?"Includes":"Does NOT include")+" duplicates");

    int theNQueries=inQuerySet.getNVectors();    
    log("  "+theNQueries+" queries from "+inQuerySet.getStandardFilename());

    // Warmup      
    log("\nIndex Warmup Search"); 
    SearchResultSet theSearchResultSet=IndexSearch.searchSet(
        inIndex,
        inSearchNNear,
        inIncludeDups,
        inQuerySet);

    // Avg of the max of (5 runs or till 2mins passed)
    log("\nIndex Search Timing Stats"); 
    int theNRuns=0;
    double theSum=0;
    long theEndTime=System.currentTimeMillis()+2*60*1000;
    while (true) {
      theSearchResultSet=IndexSearch.searchSet(theSearchResultSet);
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
      String  inIndexFilename, 
      String  inQuerySetFilename, 
      String  inSearchNNear, 
      String  inIncludeDups) throws Exception {

    long theStartTime=System.currentTimeMillis();
    log(reportHeader("Time Index Search",theStartTime));  

    Index theIndex;
    DataSet theQuerySet;
    int theSearchNNear;
    boolean theIncludeDups;

    if (kOnDevBox) {
      theIndex=Index.load("GIST_train_960D_1000Kv_30Nr");
      theQuerySet=DataSet.load("GIST_test_960D_1000v");
      theSearchNNear=40;
      theIncludeDups=false;

    } else {
      theIndex=Index.load(inIndexFilename);
      theQuerySet=DataSet.load(inQuerySetFilename);
      theSearchNNear=Integer.parseInt(inSearchNNear);
      theIncludeDups=Boolean.parseBoolean(inIncludeDups);
    }

    timeIndexSearch(theIndex,theSearchNNear,theIncludeDups,theQuerySet);

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
      String theSearchNNear=null;
      if (inArgs.length>2)
        theSearchNNear=inArgs[2];
      String theIncludeDups="false";
      if (inArgs.length>3)
        theIncludeDups=inArgs[3];
      run(theIndexFilename,theQuerySetFilename,theSearchNNear,theIncludeDups);
    } catch (Throwable e) {
      e.printStackTrace(System.err);
    }
  }
  
}

