package scala

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import HelperUtils.Constants
import com.typesafe.config.ConfigFactory

class MRTesterClass extends AnyFlatSpec with Matchers {
  behavior of "Various helper methods in constants file"

  it should "should get correct timeInterval from Input Time for a timeInterval of 1" in {
    val inputTime = "14:23".split(":")
    val testVal = Constants.generateTimeInterval(inputTime)
    "14:23 14:24" shouldBe testVal
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

  it should "the user defined config parameters should match the one in Constants" in {
    val config = ConfigFactory.load("application.conf").getConfig("userDefinedInputs")
    val checkerVal = Constants.config
    config shouldBe checkerVal
  }

  it should "is sort happening based on second val" in {
    val exampleList: List[(String, Int)] = List(("Mouli", 5), ("EarthPlaceHere",14),("CS441Cloud", 10))
    List(("Mouli", 5), ("CS441Cloud", 10), ("EarthPlaceHere",14)) shouldBe Constants.sortBasedOnSecondVal(exampleList)
  }

  it should "fetch correct intermediate filename for job2" in {
    val fileName = Constants.output0
    "_ascsort" shouldBe fileName
  }
}
