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
  
}
