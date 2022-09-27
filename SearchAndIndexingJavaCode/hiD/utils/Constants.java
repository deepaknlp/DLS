//--------------------------------------------------------------------------------------------------------
// Constants.java
//--------------------------------------------------------------------------------------------------------

package hiD.utils;

import java.awt.*;

//--------------------------------------------------------------------------------------------------------
// Constants
//--------------------------------------------------------------------------------------------------------

public interface Constants {

  public static final int        kNotFound=-1;

  public static final int        kNCores=Runtime.getRuntime().availableProcessors();
  
  // For our purposes, headless means big computer for production calcs
  // Not headless means smaller development PC for development, testing, and generating pretty figures
  public static final boolean    kOnDevBox=!GraphicsEnvironment.isHeadless();

  public static final int        kIOBufferSize=65536;

  // Run options - all of these slow down the code dramitically - turn off for production
  public static final boolean    kSanityCheck=false;
  public static final boolean    kDebug=false;
  public static final boolean    kVerboseDebug=false;
  
  // Vector flags
  public static final int        kNoFlags=0;
  public static final int        kIsNodeFlag=1;
  public static final int        kIsDupFlag=2;

  // Task stats
  public static final int        kIdleState=0;
  public static final int        kWorkingState=1;
  public static final int        kDoneState=2;

  // Project dirs
  public static final String     kProjectDir=(kOnDevBox?"D:/DataWorkspace/HiD":"/data/rloane/hiD");
  public static final String     kSourceDir=kProjectDir+"/Sources";
  public static final String     kDataSetDir=kProjectDir+"/DataSets";
  public static final String     kIndexDir=kProjectDir+"/Indexes";
  public static final String     kCurveDir=kProjectDir+"/Curves";

}

