package scala

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import HelperUtils.Constants

class MRTesterClass extends AnyFlatSpec with Matchers {
  behavior of "Config File and Map Reduce Output"

  it should "should get correct timeInterval from Input Time for a timeInterval of 5" in {
    val inputTime = "14:23".split(":")
    val testVal = Constants.generateTimeInterval(inputTime)
    "14:20 14:25" shouldBe testVal
  }

  it should "Return correct time format when give a string time format" in {
    val timeInput = "8:8"
    val testVal = Constants.convertToTimeStampInterval(timeInput)
    "08:08" shouldBe testVal
  }


  it should "should match correct message level fetched" in {
    val errorlevel = Constants.messageLevelError
    val infolevel = Constants.messageLeveInfo
    val warnlevel = Constants.messageLevelWarn
    val debuglevel = Constants.messageLevelDebug

    "INFO" shouldBe infolevel
    "ERROR" shouldBe errorlevel
    "WARN" shouldBe warnlevel
    "DEBUG" shouldBe debuglevel
  }
  
}
