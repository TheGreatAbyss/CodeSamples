package com.intentmedia.air.RollingBR

import com.intentmedia.air.RollingBR.BaseObjects.EVENT_1Key
import org.joda.time.DateTime

/**
 * Created by eric.abis on 10/14/15.
 */
object TestBuilders {
  def buildMinEVENT_1KeyRequestedAtTuple(product_category_type: String, requestedAt:DateTime):(EVENT_1Key, DateTime) = {
    (EVENT_1Key(product_category_type, 7, "String_Field_1", "String_Field_2", "String_Field_3", "String_Field_4",
      "PUID"), requestedAt)
  }
}
