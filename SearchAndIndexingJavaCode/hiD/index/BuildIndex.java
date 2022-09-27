//--------------------------------------------------------------------------------------------------------
// BuildIndex.java
//--------------------------------------------------------------------------------------------------------

package hiD.index;

import hiD.data.*;
import hiD.index.extras.IndexAccuracyTest;
import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// BuildIndex
//--------------------------------------------------------------------------------------------------------

public class BuildIndex extends FormatUtils {

//--------------------------------------------------------------------------------------------------------
// BuildIndex class vars
//--------------------------------------------------------------------------------------------------------

  private static DataSet                 gDataSet;
  private static int                     gIndexNNear;

  private static long                    gStartTime;
  private static NeighborSet             gNeighborSet;
  private static CreateHeap              gCreateHeap;
  private static CreateHeap.CreateInfo   gCreateInfo;

//--------------------------------------------------------------------------------------------------------
// buildIndex
//--------------------------------------------------------------------------------------------------------

  public static Index buildIndex(DataSet inDataSet, int inIndexNNear) throws Exception {

    gDataSet=inDataSet;
    gIndexNNear=inIndexNNear;
    gStartTime=System.currentTimeMillis(); 

    int theNVectors=gDataSet.getNVectors();
    gCreateHeap=new CreateHeap(inDataSet);
    IndexVector.open(gCreateHeap,inIndexNNear);
    gNeighborSet=new NeighborSet(theNVectors);
    gCreateInfo=new CreateHeap.CreateInfo();

    long theFindeNodesStartTime=System.currentTimeMillis();
    long theStepStartTime=theFindeNodesStartTime;
    long theStepStartNCalcs=0;
    long theStepStartNUsefulCalcs=0;
    
    log("\nIndex NNear  "+gIndexNNear);
    log("NCores       "+kNCores);

    log("\n                    "+
        leftPad("Create",13)+
        leftPad("Avg Near",12)+
        leftPad("Create",14)+
        leftPad("Useful",12)+
        leftPad("Create",12)+
        leftPad("Total",14));
    log(leftPad("NNodes    ",20)+
        leftPad("Distance2",13)+
        leftPad("Distance2",12)+
        leftPad("Calcs",14)+
        leftPad("Calcs",12)+
        leftPad("Time",12)+
        leftPad("Time",14));

    // Create nodes in the index tree in order
    int theCreateDx=0;
    int theReportStep=10;
    int theNextReportStep=20;
    for (int i=0; i<theNVectors; i++) {
      
      // The create heap tracks which vector is furthest from all existing nodes
      // The vector that is furthest becomes a newly created node 
      // Save create vectors and distances in order of creation
      gCreateHeap.getCreateInfo(gCreateInfo);
      int theCreateVectorDx=gCreateInfo.getVectorDx();
      float theCreateDistance2=gCreateInfo.getDistance2();
      
      // Don't know ahead of time when we will finish because an unknown number of dups are created in several places
      // Sure we are finished when create distance drops to zero
      if (theCreateDistance2==0)
        break;
            
      IndexVector.getIndexVector(theCreateVectorDx).createLinks(gNeighborSet);
      theCreateDx++;

      // Report progress
      if (theCreateDx%theReportStep==0) {
        long theCurrentTime=System.currentTimeMillis();
        float theAvgNearDistance2=IndexVector.calcAvgNearDistance2();
        long theNCalcs=IndexVector.getNCalcs();
        long theNUsefulCalcs=IndexVector.getNUsefulCalcs();
        log(leftPad(theCreateDx,10)+
            leftPad(formatPercent((theCreateDx)/(double) theNVectors),10)+
            leftPad(formatDistance2(theCreateDistance2),13)+
            leftPad(formatDistance2(theAvgNearDistance2),12)+ 
            leftPad(formatDouble((theNCalcs-theStepStartNCalcs)/(double) theReportStep,1),14)+
            leftPad(formatPercent((theNUsefulCalcs-theStepStartNUsefulCalcs)/(double) (theNCalcs-theStepStartNCalcs)),12)+
            leftPad(formatDuration((theCurrentTime-theStepStartTime)/(double) theReportStep),12)+
            leftPad(formatDuration(theCurrentTime-theFindeNodesStartTime),14));
        if ((theCurrentTime-theStepStartTime<5000)&&(theCreateDx%theNextReportStep==0)) {
          theReportStep=theNextReportStep;
          while (theNextReportStep>10)
            theNextReportStep/=10;
          if (theNextReportStep!=2)
            theNextReportStep=theReportStep*2;
          else
            theNextReportStep=theReportStep/2*5;
        }
        theStepStartNCalcs=theNCalcs;
        theStepStartNUsefulCalcs=theNUsefulCalcs;
        theStepStartTime=theCurrentTime;
      }
    }

    // End of finding links
    //-----------------------------------------------------------------------------------------
    // From here on is cleanup

    // Get dups
    int theNDups=IndexVector.getNDups();
    int[] theDupVectorDxs=IndexVector.getDupVectorDxs();
    int[] theDupOfVectorDxs=IndexVector.getDupOfVectorDxs();
    if (theCreateDx+theNDups!=theNVectors)
      throw new RuntimeException("Lost "+(theNVectors-theCreateDx-theNDups)+" vectors");

    // Keep nearest neighbor links
    // Loop over all nodes that are not dups - every vector is a node 
    for (int i=0; i<theNVectors; i++) {
      IndexVector theIndexVector=IndexVector.getIndexVector(i);
      if (theIndexVector.getFarLinkVectorDxs()!=null)
        theIndexVector.keepLinks(false);
    }

    // Get links
    int[][] theFarLinkVectorDxss=new int[theNVectors][];
    float[][] theFarLinkDistance2ss=new float[theNVectors][];
    int[][] theNearLinkVectorDxss=new int[theNVectors][];
    float[][] theNearLinkDistance2ss=new float[theNVectors][];
    for (int i=0; i<theNVectors; i++) {
      IndexVector theIndexVector=IndexVector.getIndexVector(i);
      theFarLinkVectorDxss[i]=theIndexVector.getFarLinkVectorDxs();
      theFarLinkDistance2ss[i]=theIndexVector.getFarLinkDistance2s();
      theNearLinkVectorDxss[i]=theIndexVector.getNearLinkVectorDxs();
      theNearLinkDistance2ss[i]=theIndexVector.getNearLinkDistance2s();
      if ((theFarLinkVectorDxss[i]==null)!=(theNearLinkVectorDxss[i]==null))
        throw new RuntimeException("Near and far links inconsistent for vector "+i);
    }
   
    // Free objects only needed to find links 
    if (gDataSet.getNDims()!=2) {
      IndexVector.close();
      gCreateHeap=null;
      gNeighborSet=null;
      gCreateInfo=null;
    }

    // Keep stats to report at end
    long theNCalcs=IndexVector.getNCalcs();
    long theNUsefulCalcs=IndexVector.getNUsefulCalcs();
    long theNLinksKept=IndexVector.getNLinksKept();

    // Log time
    long theElapsedTime=System.currentTimeMillis()-theFindeNodesStartTime;
    log("\nFinding links took:    "+formatDuration(theElapsedTime));
    log("Links kept             "+theNLinksKept);


    
    log("\nMerge Near Links, Far Links, and Dups");
    
    LinkChainStore theLinkChainStore=new LinkChainStore(theNVectors,(long) (2.2*theNLinksKept));

    // Merge near and far links
    // Also make links two way - if A --> B, add B --> A, and unique at end
    for (int theVectorDx=0; theVectorDx<theNVectors; theVectorDx++) {
      int[] theFarLinkVectorDxs=theFarLinkVectorDxss[theVectorDx];
      if (theFarLinkVectorDxs!=null) {  // No links for dups
        float[] theFarLinkDistance2s=theFarLinkDistance2ss[theVectorDx];
        for (int j=0; j<theFarLinkVectorDxs.length; j++) {
          int theOtherVectorDx=theFarLinkVectorDxs[j];
          float theDistance2=theFarLinkDistance2s[j];
          theLinkChainStore.appendLinkInfo(theVectorDx,theOtherVectorDx,theDistance2);
          theLinkChainStore.appendLinkInfo(theOtherVectorDx,theVectorDx,theDistance2);
        }
        theFarLinkVectorDxss[theVectorDx]=null;
        theFarLinkDistance2ss[theVectorDx]=null;
        
        int[] theNearLinkVectorDxs=theNearLinkVectorDxss[theVectorDx];
        float[] theNearLinkDistance2s=theNearLinkDistance2ss[theVectorDx];
        for (int j=0; j<theNearLinkVectorDxs.length; j++) {
          int theOtherVectorDx=theNearLinkVectorDxs[j];
          float theDistance2=theNearLinkDistance2s[j];
          theLinkChainStore.appendLinkInfo(theVectorDx,theOtherVectorDx,theDistance2);
          theLinkChainStore.appendLinkInfo(theOtherVectorDx,theVectorDx,theDistance2);
        }
        theNearLinkVectorDxss[theVectorDx]=null;
        theNearLinkDistance2ss[theVectorDx]=null;
      }
    }
    theFarLinkVectorDxss=null;
    theFarLinkDistance2ss=null;
    theNearLinkVectorDxss=null;
    theNearLinkDistance2ss=null;

    // Add dups 
    // Dups are recognizable in the index by having a single zero length link to the "real" vector they are a dup of
    for (int i=0; i<theNDups; i++) {
      int theDupVectorDx=theDupVectorDxs[i];
      int theDupOfVectorDx=theDupOfVectorDxs[i];
      if (theLinkChainStore.getNLinksInChain(theDupVectorDx)>0) 
        throw new RuntimeException("Dup vector "+theDupVectorDx+" was linked");
      
      theLinkChainStore.appendLinkInfo(theDupVectorDx,theDupOfVectorDx,0);
      theLinkChainStore.appendLinkInfo(theDupOfVectorDx,theDupVectorDx,0);
    }
    theDupVectorDxs=null;
    theDupOfVectorDxs=null;
   
    
    
    log("Sort and Unique Links");
    
    // Allocate 2D index arrays
    int[][] theLinkVectorDxss=new int[theNVectors][];
    float[][] theLinkDistance2ss=new float[theNVectors][];

    // Allocate work arrays
    int[] theVectorDxs=new int[2*theNVectors];
    float[] theDistance2s=new float[2*theNVectors];
    boolean[] theLinkExists=new boolean[theNVectors];
    
    // Loop over all vectors
    for (int i=0; i<theNVectors; i++) {

      // Get links from chain store - contains dups
      int theNLinks=theLinkChainStore.getLinkInfos(i,theVectorDxs,theDistance2s);

      // Only keep unique links by using a lookup array to check if they exist 
      int theNKept=0;
      for (int j=0; j<theNLinks; j++) {
        int theVectorDx=theVectorDxs[j];
        
        // Check lookup to see if link does not yet exist
        if (!theLinkExists[theVectorDx]) {
          
          // Set lookup so next check will fail
          theLinkExists[theVectorDx]=true;
          
          // Keep uniqued link and distance
          theVectorDxs[theNKept]=theVectorDx;
          theDistance2s[theNKept]=theDistance2s[j];
          theNKept++;                    
        }
      }
      theNLinks=theNKept;
      
      // Clear lookup for reuse
      for (int j=0; j<theNLinks; j++)
        theLinkExists[theVectorDxs[j]]=false;

      // Allocate room in index for link VectorDxs and Distance2s
      int[] theLinkVectorDxs=new int[theNLinks];
      float[] theLinkDistance2s=new float[theNLinks];
      theLinkVectorDxss[i]=theLinkVectorDxs;
      theLinkDistance2ss[i]=theLinkDistance2s;

      // Sort by Distance2 
      float[] theTempDistance2s=new float[theNLinks];
      System.arraycopy(theDistance2s,0,theTempDistance2s,0,theNLinks);
      int[] theSortMap=SortUtils.sortMap(theTempDistance2s,false);      
      
      // Apply sort order and store links in 2D index array
      for (int j=0; j<theNLinks; j++) {
        int jj=theSortMap[j];
        theLinkVectorDxs[j]=theVectorDxs[jj];
        theLinkDistance2s[j]=theDistance2s[jj];
      }     
    }

    // Free work arrays
    theLinkChainStore=null;
    theVectorDxs=null;
    theDistance2s=null;
    theLinkExists=null;

    // Create and save index
    long theIndexingTime=System.currentTimeMillis()-gStartTime;
    Index theIndex=new Index(
        gDataSet, 
        gIndexNNear,
        theIndexingTime,
        theLinkVectorDxss,
        theLinkDistance2ss);
    theIndex.save();

    // Test index accuracy
    IndexAccuracyTest.testAccuracy(theIndex);

    // Report stats
    log("\nIndex:            "+theIndex.getStandardFilename());
    log("Indexing time:    "+leftPad(formatDuration(theIndexingTime),16));
    log("NVectors          "+leftPad(theNVectors,16));
    log("NDups             "+leftPad(theNDups,16)+
        leftPad("("+formatPercent(theNDups/(double) theNVectors)+")",12));
    log("IndexNNear        "+leftPad(gIndexNNear,16));
    log("NCalcs            "+leftPad(theNCalcs,16)+
        leftPad(theNCalcs/theNVectors,12)+" per node");
    log("NUsefulCalcs      "+leftPad(theNUsefulCalcs,16)+
        leftPad("("+formatPercent(theNUsefulCalcs/(double) theNCalcs)+")",12));
    log("NWastedCalcs      "+leftPad(theNCalcs-theNUsefulCalcs,16)+
        leftPad("("+formatPercent((theNCalcs-theNUsefulCalcs)/(double) theNCalcs)+")",12));
    log("NLinks:           "+leftPad(theIndex.getTotalNLinks(),16)+
        leftPad(formatDouble(theIndex.getTotalNLinks()/(double) theNVectors),12)+" per node");
    
    return theIndex;
  }

//--------------------------------------------------------------------------------------------------------
// run 
//--------------------------------------------------------------------------------------------------------

  public static void run(String inDataSetFilename, String inIndexNNear) throws Exception {

    long theStartTime=System.currentTimeMillis();
    log(reportHeader("Build HiD Search Index",theStartTime));
 
    if (kOnDevBox) {  
      
      buildIndex(DataSet.load("Artificial_train_40D_9000v"),20);
      buildIndex(DataSet.load("Faces_train_20D_9304v"),20);     
      buildIndex(DataSet.load("Corel_train_32D_58Kv"),20);      
      buildIndex(DataSet.load("CovType_train_54D_571Kv"),20);   
      buildIndex(DataSet.load("TinyImages_train_384D_90Kv"),20);
      buildIndex(DataSet.load("Twitter_train_78D_573Kv"),20);   
      buildIndex(DataSet.load("YearPred_train_90D_505Kv"),20);  
      buildIndex(DataSet.load("MNIST_train_784D_60Kv"),20);
      buildIndex(DataSet.load("FMNIST_train_784D_60Kv"),20);
      buildIndex(DataSet.load("SIFT_train_128D_1000Kv"),20);
      buildIndex(DataSet.load("GIST_train_960D_1000Kv"),20);  

    } else {  
      buildIndex(DataSet.load(inDataSetFilename),
                 Integer.parseInt(inIndexNNear));
    }
    
    log(reportFooter(theStartTime));
    Thread.sleep(1000);
  }
 
//--------------------------------------------------------------------------------------------------------
// main 
//--------------------------------------------------------------------------------------------------------

  public static void main(String[] inArgs) {
    try {
      String theDataSetFilename=null;
      if (inArgs.length>0)
        theDataSetFilename=inArgs[0];
      String theIndexNNear=null;
      if (inArgs.length>1)
        theIndexNNear=inArgs[1];
      BuildIndex.run(theDataSetFilename,theIndexNNear);
    } catch (Throwable e) {
      e.printStackTrace(System.err);
    }
  }
  
}

 