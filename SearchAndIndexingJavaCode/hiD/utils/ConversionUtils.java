//--------------------------------------------------------------------------------------------------------
// ConversionUtils.java
//--------------------------------------------------------------------------------------------------------

package hiD.utils;

//--------------------------------------------------------------------------------------------------------
// ConversionUtils
//--------------------------------------------------------------------------------------------------------

public class ConversionUtils implements Constants {
  
//--------------------------------------------------------------------------------------------------------
// ConversionUtils consts
//--------------------------------------------------------------------------------------------------------

  public static final int        kByteMemory=1;
  public static final int        kBooleanMemory=1;
  public static final int        kCharMemory=2;
  public static final int        kShortMemory=2;
  public static final int        kIntMemory=4;
  public static final int        kFloatMemory=4;
  public static final int        kLongMemory=8;
  public static final int        kDoubleMemory=8;
  
  
  
//--------------------------------------------------------------------------------------------------------
//
// To bytes
//
//--------------------------------------------------------------------------------------------------------


//--------------------------------------------------------------------------------------------------------
// booleanToBytes
//--------------------------------------------------------------------------------------------------------

  public static void booleanToBytes(boolean inBoolean, byte[] ioBytes, int inByteDelta) {
    if (inBoolean)
      ioBytes[inByteDelta]=(byte) -1;
    else
      ioBytes[inByteDelta]=0;
  }

//--------------------------------------------------------------------------------------------------------
// shortToBytes
//--------------------------------------------------------------------------------------------------------

  public static void shortToBytes(short inShort, byte[] ioBytes, int inByteDelta) {
    ioBytes[inByteDelta]=(byte) (inShort>>>8);
    ioBytes[inByteDelta+1]=(byte) inShort;
  }

//--------------------------------------------------------------------------------------------------------
// intToBytes
//--------------------------------------------------------------------------------------------------------

  public static void intToBytes(int inInt, byte[] ioBytes, int inByteDelta) {
    int theInt=inInt;
    int theByteEnd=inByteDelta+kIntMemory;
    for (int i=theByteEnd-1; i>=inByteDelta; i--) {
      ioBytes[i]=(byte) theInt;
      theInt>>>=8;
    }
  }

//--------------------------------------------------------------------------------------------------------
// longToBytes
//--------------------------------------------------------------------------------------------------------

  public static void longToBytes(long inLong, byte[] ioBytes, int inByteDelta) {
    long theLong=inLong;
    int theByteEnd=inByteDelta+kLongMemory;
    for (int i=theByteEnd-1; i>=inByteDelta; i--) {
      ioBytes[i]=(byte) theLong;
      theLong>>=8;
    }
  }

//--------------------------------------------------------------------------------------------------------
// floatToBytes
//--------------------------------------------------------------------------------------------------------

  public static void floatToBytes(float inFloat, byte[] ioBytes, int inByteDelta) {
    intToBytes(Float.floatToIntBits(inFloat),ioBytes,inByteDelta); }

//--------------------------------------------------------------------------------------------------------
// doubleToBytes
//--------------------------------------------------------------------------------------------------------

  public static void doubleToBytes(double inDouble, byte[] ioBytes, int inByteDelta) {
    longToBytes(Double.doubleToLongBits(inDouble),ioBytes,inByteDelta); }

//--------------------------------------------------------------------------------------------------------
// intsToBytes
//--------------------------------------------------------------------------------------------------------

  public static void intsToBytes(int[] inInts, int inIntDelta, int inNInts, byte[] ioBytes, int inByteDelta) {
    int theByteDelta=inByteDelta;
    int theEndInt=inIntDelta+inNInts;
    for (int i=inIntDelta; i<theEndInt; i++) {
      intToBytes(inInts[i],ioBytes,theByteDelta);
      theByteDelta+=kIntMemory;
    }
  }

//--------------------------------------------------------------------------------------------------------
// floatsToBytes
//--------------------------------------------------------------------------------------------------------

  public static void floatsToBytes(float[] inFloats, int inFloatDelta, int inNFloats, byte[] ioBytes,
          int inByteDelta) {
    int theByteDelta=inByteDelta;
    int theEndFloat=inFloatDelta+inNFloats;
    for (int i=inFloatDelta; i<theEndFloat; i++) {
      floatToBytes(inFloats[i],ioBytes,theByteDelta);
      theByteDelta+=kFloatMemory;
    }
  }

  
  
//--------------------------------------------------------------------------------------------------------
//
// From bytes
//
//--------------------------------------------------------------------------------------------------------
 
//--------------------------------------------------------------------------------------------------------
// bytesToBoolean
//--------------------------------------------------------------------------------------------------------

  public static boolean bytesToBoolean(byte[] inBytes, int inByteDelta) {
    return ((inBytes[inByteDelta]&1)!=0); }

//--------------------------------------------------------------------------------------------------------
// bytesToShort
//--------------------------------------------------------------------------------------------------------

  public static short bytesToShort(byte[] inBytes, int inByteDelta) {
    return (short) (((inBytes[inByteDelta]&0x00ff)<<8)|(inBytes[inByteDelta+1]&0x00ff)); }

//--------------------------------------------------------------------------------------------------------
// bytesToInt
//--------------------------------------------------------------------------------------------------------

  public static int bytesToInt(byte[] inBytes, int inByteDelta) {
    int theInt=inBytes[inByteDelta]; // Sign transfer ocurrs here
    int theByteEnd=inByteDelta+4;
    for (int i=inByteDelta+1; i<theByteEnd; i++) {
      theInt<<=8;
      theInt|=(inBytes[i]&0x00ff);
    }
    return theInt;
  }

//--------------------------------------------------------------------------------------------------------
// bytesToLong
//--------------------------------------------------------------------------------------------------------

  public static long bytesToLong(byte[] inBytes, int inByteDelta) {
    long theLong=inBytes[inByteDelta]; // Sign transfer ocurrs here
    int theByteEnd=inByteDelta+8;
    for (int i=inByteDelta+1; i<theByteEnd; i++) {
      theLong<<=8;
      theLong|=(inBytes[i]&0x00ff);
    }
    return theLong;
  }

//--------------------------------------------------------------------------------------------------------
// bytesToFloat
//--------------------------------------------------------------------------------------------------------

  public static float bytesToFloat(byte[] inBytes, int inByteDelta) {
    return Float.intBitsToFloat(bytesToInt(inBytes,inByteDelta)); }

//--------------------------------------------------------------------------------------------------------
// bytesToDouble
//--------------------------------------------------------------------------------------------------------

  public static double bytesToDouble(byte[] inBytes, int inByteDelta) {
    return Double.longBitsToDouble(bytesToLong(inBytes,inByteDelta)); }

//--------------------------------------------------------------------------------------------------------
// bytesToInts
//--------------------------------------------------------------------------------------------------------

  public static void bytesToInts(byte[] inBytes, int inByteDelta, int inNBytes, int[] ioInts,
          int inIntDelta) {
    int theByteDelta=inByteDelta;
    int theEndInt=inIntDelta+inNBytes/kIntMemory;
    for (int i=inIntDelta; i<theEndInt; i++) {
      ioInts[i]=bytesToInt(inBytes,theByteDelta);
      theByteDelta+=kIntMemory;
    }
  }

//--------------------------------------------------------------------------------------------------------
// bytesToFloats
//--------------------------------------------------------------------------------------------------------
  
  public static void bytesToFloats(byte[] inBytes, int inByteDelta, int inNBytes, float[] ioFloats,
          int inFloatDelta) {
    int theByteDelta=inByteDelta;
    int theEndFloat=inFloatDelta+inNBytes/kFloatMemory;
    for (int i=inFloatDelta; i<theEndFloat; i++) {
      ioFloats[i]=bytesToFloat(inBytes,theByteDelta);
      theByteDelta+=kFloatMemory;
    }
  }

}





