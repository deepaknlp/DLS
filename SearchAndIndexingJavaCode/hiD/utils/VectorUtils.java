//--------------------------------------------------------------------------------------------------------
// VectorUtils.java
//--------------------------------------------------------------------------------------------------------

package hiD.utils;

import java.util.Random;

//--------------------------------------------------------------------------------------------------------
// VectorUtils
//--------------------------------------------------------------------------------------------------------

public class VectorUtils implements Constants {
  
//--------------------------------------------------------------------------------------------------------
// addVectors
//--------------------------------------------------------------------------------------------------------

  // This version of routine requires a pre-allocated sum vector
  public static void addVectors(float[] inVector1, float[] inVector2, float[] outSumVector) {
    if ((inVector1.length!=inVector2.length)||(inVector1.length!=outSumVector.length))
      throw new RuntimeException("Vectors different NDims");
    for (int i=0; i<inVector1.length; i++) 
      outSumVector[i]=inVector1[i]+inVector2[i];
  }

//--------------------------------------------------------------------------------------------------------
// subtractVectors
//--------------------------------------------------------------------------------------------------------

  public static void subtractVectors(float[] inVector1, float[] inVector2, float[] outDifferenceVector) {
    if ((inVector1.length!=inVector2.length)||(inVector1.length!=outDifferenceVector.length))
      throw new RuntimeException("Vectors different NDims");
    for (int i=0; i<inVector1.length; i++) 
      outDifferenceVector[i]=inVector1[i]-inVector2[i];
  }

//--------------------------------------------------------------------------------------------------------
// scaleVector
//--------------------------------------------------------------------------------------------------------

  public static void scaleVector(double inScale, float[] inVector, float[] outScaledVector) {
    for (int i=0; i<inVector.length; i++) 
      outScaledVector[i]=(float) (inScale*inVector[i]);
  }

//--------------------------------------------------------------------------------------------------------
// vectorLength2
//--------------------------------------------------------------------------------------------------------

  public static double vectorLength2(float[] inVector) {
    double theLength2=0;
    for (int i=0; i<inVector.length; i++) {
      double theComponent=inVector[i];
      theLength2+=theComponent*theComponent;
    }
    return theLength2;
  }

//--------------------------------------------------------------------------------------------------------
// vectorLength
//--------------------------------------------------------------------------------------------------------

  public static double vectorLength(float[] inVector) {
    return Math.sqrt(vectorLength2(inVector)); }

//--------------------------------------------------------------------------------------------------------
// vectorSeparation2
//--------------------------------------------------------------------------------------------------------

  public static double vectorSeparation2(float[] inVector1, float[] inVector2) {
    if (inVector1.length!=inVector2.length)
      throw new RuntimeException("Vectors different NDims");
    double theSeparation2=0;
    for (int i=0; i<inVector1.length; i++) { 
      double theDifference=inVector1[i]-inVector2[i];
      theSeparation2+=theDifference*theDifference;
    }
    return theSeparation2;
  }

//--------------------------------------------------------------------------------------------------------
// vectorSeparation
//--------------------------------------------------------------------------------------------------------

  public static double vectorSeparation(float[] inVector1, float[] inVector2) {
    return Math.sqrt(vectorSeparation2(inVector1,inVector2)); }

//--------------------------------------------------------------------------------------------------------
// vectorsAreDups
//--------------------------------------------------------------------------------------------------------

  public static boolean vectorsAreDups(float[] inVector1, float[] inVector2) {
    if (inVector1.length!=inVector2.length)
      throw new RuntimeException("Vectors different NDims");
    for (int i=0; i<inVector1.length; i++)
      if (inVector1[i]!=inVector2[i])
        return false;
    return true;
  }

//--------------------------------------------------------------------------------------------------------
// vectorDotProduct
//--------------------------------------------------------------------------------------------------------

  public static double vectorDotProduct(float[] inVector1, float[] inVector2) {
    if (inVector1.length!=inVector2.length)
      throw new RuntimeException("Vectors different lengths");
    double theDotProduct=0;
    for (int i=0; i<inVector1.length; i++) {
      double theComponent1=inVector1[i];
      double theComponent2=inVector2[i];
      theDotProduct+=theComponent1*theComponent2;
    }
    return theDotProduct;
  }

//--------------------------------------------------------------------------------------------------------
// vectorCosine
//--------------------------------------------------------------------------------------------------------

  public static double vectorCosine(float[] inVector1, float[] inVector2) {
    double theLengthProduct=Math.sqrt(vectorLength2(inVector1)*vectorLength2(inVector2));
    if (theLengthProduct==0.0)
      return 0.0;
    else
      return vectorDotProduct(inVector1,inVector2)/theLengthProduct;
   }

//--------------------------------------------------------------------------------------------------------
// vectorAngle
//--------------------------------------------------------------------------------------------------------

  public static double vectorAngle(float[] inVector1, float[] inVector2) {
    return Math.acos(Math.min(1.0,Math.max(-1.0,vectorCosine(inVector1,inVector2)))); }

//--------------------------------------------------------------------------------------------------------
// randomNormal
//--------------------------------------------------------------------------------------------------------

  private static final double   kNotAvailable=Double.MAX_VALUE;
  private static final Random   kRandomGenerator=new Random();

  private static double   gLastNormalDeviate=kNotAvailable;

  public static double randomNormal() {

    // Normal deviates are generated in pairs - check to see if one is available
    if (gLastNormalDeviate!=kNotAvailable) {
      double theLastNormalDeviate=gLastNormalDeviate;
      gLastNormalDeviate=kNotAvailable;      
      return theLastNormalDeviate;

    // Generate the next pair of normal deviates
    } else {
      double theU;
      double theV;
      double theS2;
      do {
        theU=2.0*kRandomGenerator.nextDouble()-1.0;
        theV=2.0*kRandomGenerator.nextDouble()-1.0;
        theS2=theU*theU+theV*theV;
      } while ((theS2>1.0)||(theS2==0.0));
      double theROvrS=Math.sqrt(-2.0*Math.log(theS2)/theS2);
      double theNormalDeviate=theV*theROvrS;
      gLastNormalDeviate=-theU*theROvrS;      
      return theNormalDeviate;
    }
  }

//--------------------------------------------------------------------------------------------------------
// randomNormalVector
//--------------------------------------------------------------------------------------------------------

  public static void randomNormalVector(float[] outVector) {   
    for (int i=0; i<outVector.length; i++) 
      outVector[i]=(float) randomNormal();
  }

//--------------------------------------------------------------------------------------------------------
// randomUnitVector
//--------------------------------------------------------------------------------------------------------

  public static void randomUnitVector(float[] outVector) {    
    double theLength=0.0;
    do {
      randomNormalVector(outVector);
      theLength=vectorLength(outVector);
    } while (theLength<0.01);
    scaleVector(1.0/theLength,outVector,outVector);
  }

}

