//--------------------------------------------------------------------------------------------------------
// BuildDataSet.java
//--------------------------------------------------------------------------------------------------------

package hiD.data;

import java.io.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import hiD.utils.*;

//--------------------------------------------------------------------------------------------------------
// BuildDataSet
//--------------------------------------------------------------------------------------------------------

public class BuildDataSet extends FormatUtils {

//--------------------------------------------------------------------------------------------------------
// buildDataSet
//--------------------------------------------------------------------------------------------------------

  public DataSet buildDataSet(
      int         inNDims,
      int         inNVectors,
      String      inSourceName,
      float[][]   inVectors, 
      String[]    inDescriptors,
      boolean     inNormalize) throws Exception {
      
    double[] theSum=new double[inNDims]; 
    for (int i=0; i<inNVectors; i++) 
      for (int j=0; j<inNDims; j++) 
        theSum[j]+=inVectors[i][j];
      
    float[] theMean=new float[inNDims]; 
    for (int j=0; j<inNDims; j++) 
      theMean[j]=(float) (theSum[j]/inNVectors);
        
    if (inNormalize) {
      log("  Subtracting out mean so centered at origin");
      log("    Mean length: "+formatDistance2(VectorUtils.vectorLength(theMean)));
      for (int i=0; i<inNVectors; i++) 
        VectorUtils.subtractVectors(inVectors[i],theMean,inVectors[i]);
    } else 
      log("  Mean length: "+formatDistance2(VectorUtils.vectorLength(theMean))+
          "    Prefer 0.0 so centered at origin");
    
    double theVariance=0;
    double theMaxLength2=0;
    for (int theVectorDx=0; theVectorDx<inNVectors; theVectorDx++) {
      double theLength2=VectorUtils.vectorLength2(inVectors[theVectorDx]);
      theVariance+=theLength2;
      theMaxLength2=Math.max(theMaxLength2,theLength2);
    }
    theVariance/=inNVectors;

    double theStdLengthScale=Math.sqrt(inNDims);
    double theScale=theStdLengthScale/Math.sqrt(theVariance);
    double theMaxLengthScale=theScale*Math.sqrt(theMaxLength2);
       
    if (inNormalize) {
      log("  Scaling radial std dev to √"+inNDims+" = "+
          formatDistance2(theStdLengthScale)+" so components are all ~1");
      log("    Scale: "+formatDistance2(theScale));
      for (int i=0; i<inNVectors; i++) 
        VectorUtils.scaleVector(theScale,inVectors[i],inVectors[i]);
      
    } else 
      log("  Radial std dev: "+formatDistance2(Math.sqrt(theVariance))+
          "    Prefer √"+inNDims+" = "+formatDistance2(theStdLengthScale)+" so components are all ~1");

    if (inNormalize) {
      // Randomize vector order 
      // For each vector, swap it with a random vector with equal or larger index
      log("  Randomizing vector order");
      Random theGenerator=new Random();
      for (int theVectorDx=0; theVectorDx<inNVectors; theVectorDx++) {
        int j=theVectorDx+(int) Math.floor((inNVectors-theVectorDx)*theGenerator.nextDouble());
        if (theVectorDx!=j) {  // If self, no need to swap
          // Swap
          float[] theVector=inVectors[theVectorDx];
          inVectors[theVectorDx]=inVectors[j];
          inVectors[j]=theVector;
          if (inDescriptors!=null) {
            String theDescriptor=inDescriptors[theVectorDx];
            inDescriptors[theVectorDx]=inDescriptors[j];
            inDescriptors[j]=theDescriptor;
          }
        }
      }
    }    

    String theSourceName=extractSourceName(inSourceName);

    DataSet theDataSet=new DataSet(
        inNDims,
        inNVectors,
        theSourceName,
        theMaxLengthScale,
        theMean,
        theScale,
        inVectors,
        inDescriptors);
    
    theDataSet.save();
    
    return theDataSet;
  }

//--------------------------------------------------------------------------------------------------------
// breakOnChars
//--------------------------------------------------------------------------------------------------------

  public static String[] breakOnChars(String inString, char inChar) {

    if ((inString==null)||(inString.length()==0))
      return new String[0];

    int theNTokens=1;
    int thePos=inString.indexOf(inChar);
    while (thePos>=0) {
      theNTokens++;
      thePos=inString.indexOf(inChar,thePos+1);
    }

    String[] theTokens=new String[theNTokens];

    theNTokens=0;
    int theLastPos=0;
    thePos=inString.indexOf(inChar);
    while (thePos>=0) {
      theTokens[theNTokens]=inString.substring(theLastPos,thePos);
      theNTokens++;
      theLastPos=thePos+1;
      thePos=inString.indexOf(inChar,theLastPos);
    }
    theTokens[theNTokens]=inString.substring(theLastPos);

    return theTokens;
  }

//--------------------------------------------------------------------------------------------------------
// buildCSVDataSet
//--------------------------------------------------------------------------------------------------------

  public DataSet buildCSVDataSet(String inSourceFilename, boolean inNomalize) throws Exception {
    
    log("\nLoading data from "+inSourceFilename);

    int theNDims=0;
    String theSourceName=extractSourceName(inSourceFilename);
    ArrayList theVectorList=new ArrayList();
    
    BufferedReader theReader=FileUtils.openInputReader(inSourceFilename); 
    try {
      
      String theLine=theReader.readLine();
      while (theLine!=null) {
        theLine=theLine.trim();
        if (theLine.length()>0) {
          String[] theComps=breakOnChars(theLine,',');
          
          if (theNDims==0)
            theNDims=theComps.length;
          else if (theComps.length!=theNDims)
            throw new RuntimeException("NDims changed from "+theNDims+" to "+theComps.length);
  
          float[] theVector=new float[theNDims];
  
          for (int i=0; i<theComps.length; i++) {
            String theComp=theComps[i];
            int theStart=0;
            char theChar=theComp.charAt(theStart);
            while ((theChar==' ')||(theChar=='"')) {
              theStart++;
              theChar=theComp.charAt(theStart);
            }
            int theEnd=theComp.length();
            theChar=theComp.charAt(theEnd-1);
            while ((theChar==' ')||(theChar=='"')) {
              theEnd--;
              theChar=theComp.charAt(theEnd-1);
            }
            theVector[i]=Float.parseFloat(theComps[i]);
          }
          theVectorList.add(theVector);
          
          if (theVectorList.size()%10000==0)
            log("    "+theVectorList.size()+" vectors");
          theLine=theReader.readLine();
        }
      }
      
    } finally {
      theReader.close();
    }
    
    int theNVectors=theVectorList.size();
    float[][] theVectors=(float[][]) theVectorList.toArray(new float[theNVectors][]);
    String[] theDescriptors=null;

    // Stats
    long theFileSize=FileUtils.getFileSize(inSourceFilename);
    log("  "+
        theNVectors+" vectors with "+
        theNDims+" dimensions, "+
        formatMemory(theFileSize)+" on disk");

    return buildDataSet(
        theNDims,
        theNVectors,
        theSourceName,
        theVectors,
        theDescriptors,
        inNomalize);
 }
  
//--------------------------------------------------------------------------------------------------------
// buildOpenIDataSet
//--------------------------------------------------------------------------------------------------------

  public DataSet buildOpenIDataSet(String inSourceFilename, boolean inNomalize) throws Exception {
    
    int theNDims=0;
    int theNErrors=0;
    ArrayList theVectorList=new ArrayList();
    ArrayList theDescriptorList=new ArrayList();
    HashSet theDescriptor1Lookup=new HashSet();
    
    log("\nLoading OpenI data from "+inSourceFilename);
    BufferedReader theReader=FileUtils.openInputReader(inSourceFilename); 
    try {
      
      String theLine=theReader.readLine();
      while (theLine!=null) {
        theLine=theLine.trim();
        if (theLine.length()>0) {
          
          // Line has descriptor, tab, comma separated list of 512 components
          int thePos=theLine.indexOf('\t');
          if (thePos==kNotFound) {
            log("Bad line: Missing tab separator\n"+theLine);
            theNErrors++;
          } else {
            
            String theDescriptor=theLine.substring(0,thePos);
            if (theDescriptor1Lookup.contains(theDescriptor)) {
              log("Bad line: Duplicate descriptor - vector dropped\n"+theLine);
              theNErrors++;
            } else {
              theDescriptor1Lookup.add(theDescriptor);
              
              String theCompList=theLine.substring(thePos+1);
              String[] theComps=breakOnChars(theCompList,',');
              if (theNDims==0)
                theNDims=theComps.length;
              else if (theComps.length!=theNDims) {
                log("Bad line:  NDims changed from "+theNDims+" to "+theComps.length+"\n"+theLine);
                theNErrors++;
              } else {
                
                float[] theVector=new float[theNDims];
                int i=0;
                try {
                  for (i=0; i<theNDims; i++) 
                    theVector[i]=Float.parseFloat(theComps[i]);
                } catch (Exception e) {
                  log("Bad line: Comp "+i+" is not a parsable float "+theLine);
                  theNErrors++;
                }
                
                theVectorList.add(theVector);
                theDescriptorList.add(theDescriptor);
                if (theVectorList.size()%100000==0)
                  log("    "+theVectorList.size()+" vectors");
              }
            }
          }
        }
        theLine=theReader.readLine();
      }

    } finally {
      theReader.close();
    }
    
    theDescriptor1Lookup=null;
    
    if (theNErrors>0) 
      log("\nWARNING:  Data had "+theNErrors+" errors - consider abandoning DataSet");
    
    int theNVectors=theVectorList.size();
    float[][] theVectors=(float[][]) theVectorList.toArray(new float[theNVectors][]);
    String[] theDescriptors=null;
    if (theDescriptorList!=null)
      theDescriptors=(String[]) theDescriptorList.toArray(new String[theNVectors]);

    // Stats
    long theFileSize=FileUtils.getFileSize(inSourceFilename);
    log("  "+
        theNVectors+" vectors with "+
        theNDims+" dimensions, "+
        formatMemory(theFileSize)+" on disk");

    String theSourceName=extractSourceName(inSourceFilename);
    
    return buildDataSet(
        theNDims,
        theNVectors,
        theSourceName,
        theVectors,
        theDescriptors,
        inNomalize);
  }

//--------------------------------------------------------------------------------------------------------
// run 
//--------------------------------------------------------------------------------------------------------

  public void run(String inSourceFilename, String inNormalize) throws Exception {

    long theStartTime=System.currentTimeMillis();
    log(reportHeader("Build DataSet",theStartTime));

    // Hard coded values for convenience when run in dev envr
    if (kOnDevBox) {  
            
      boolean theNormalize=true;

      buildCSVDataSet(kSourceDir+"/Artificial/Artificial_test.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/Artificial/Artificial_train.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/Faces/Faces_test.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/Faces/Faces_train.csv",theNormalize);
      
      buildCSVDataSet(kSourceDir+"/Corel/Corel_test.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/Corel/Corel_train.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/CovType/CovType_test.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/CovType/CovType_train.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/TinyImages/TinyImages_test.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/TinyImages/TinyImages_train.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/Twitter/Twitter_test.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/Twitter/Twitter_train.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/YearPred/YearPred_test.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/YearPred/YearPred_train.csv",theNormalize);

      buildCSVDataSet(kSourceDir+"/MNIST/MNIST_test.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/MNIST/MNIST_train.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/FMNIST/FMNIST_test.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/FMNIST/FMNIST_train.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/SIFT/SIFT_test.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/SIFT/SIFT_train.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/GIST/GIST_test.csv",theNormalize);
      buildCSVDataSet(kSourceDir+"/GIST/GIST_train.csv",theNormalize);

    // Parameters passed in from command line for when run in production
    } else 
      buildOpenIDataSet(inSourceFilename,Boolean.parseBoolean(inNormalize));

    log(reportFooter(theStartTime));
  }
 
//--------------------------------------------------------------------------------------------------------
// main 
//--------------------------------------------------------------------------------------------------------

  public static void main(String[] inArgs) {
    try {
      String theSourceFilename=null;
      if (inArgs.length>0)
        theSourceFilename=inArgs[0];
      String theNormalize=null;
      if (inArgs.length>1)
        theNormalize=inArgs[1];
       new BuildDataSet().run(theSourceFilename,theNormalize);
    } catch (Throwable e) {
      e.printStackTrace(System.err);
    }
  }

}

