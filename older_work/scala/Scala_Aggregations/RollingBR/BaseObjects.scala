package com.company.air.RollingBR

import com.company.data._
import com.company.util.ConvertJava._
import org.joda.time.{DateTime, DateTimeZone}

object BaseObjects {

  case class event1(productCategoryType: String,  ID_Field_1: Int, String_Field_Type_1: String,
                    String_Field_Type_2: String, String_Field_Type_3: String, String_Field_Type_4: String,
                    ID_Field_2: String, requestedAt: DateTime, Filterable_Field: Boolean)

  object event1 {
    def apply(event1Data: event1Data): event1 = {
      new event1(
        if (event1Data.getProductCategoryType == null) "Unknown" else event1Data.getProductCategoryType.getDisplayName,
        integer2OptionInt(event1Data.getID_Field_1).getOrElse(0),
        if (event1Data.getString_Field_Type_1 == null) "Unknown" else event1Data.getString_Field_Type_1.name,
        if (event1Data.getString_Field_Type_2 == null) "Unknown" else event1Data.getString_Field_Type_2.getDisplayName,
        if (event1Data.getString_Field_Type_3 == null) "Unknown" else event1Data.getString_Field_Type_3.getDisplayName,
        if (event1Data.getString_Field_Type_4 == null) "Unknown" else event1Data.getString_Field_Type_4,
        if (event1Data.getID_Field_2 == null) "Unknown" else event1Data.getID_Field_2,
        event1Data.getRequestedAt.toDateTime(DateTimeZone.UTC),
        if (event1Data.Filterable_Field == null) false else event1Data.Filterable_Field
      )
    }
  }

  case class event1Key(productCategoryType: String,  ID_Field_1: Int, String_Field_Type_1: String,
                    String_Field_Type_2: String, String_Field_Type_3: String, String_Field_Type_4: String, ID_Field_2: String)

  object event1Key {
    def apply(event1Row: event1): event1Key = {
      new event1Key(
        event1Row.productCategoryType,
        event1Row.ID_Field_1,
        event1Row.String_Field_Type_1,
        event1Row.String_Field_Type_2,
        event1Row.String_Field_Type_3,
        event1Row.String_Field_Type_4,
        event1Row.ID_Field_2
      )
    }
  }


  case class UserMinDate(productCategoryType: String,  ID_Field_1: Int, String_Field_Type_1: String,
                         String_Field_Type_2: String, String_Field_Type_3: String, ID_Field_2: String,
                         minDate: DateTime)


  case class Event2(productCategoryType: String, ID_Field_2: String,  requestedAt: DateTime, Filterable_Field: Boolean)

  object event2 {
    def apply(event2Data: event2Data): event2 = {
      new event2(
        event2Data.getProductCategoryType.getDisplayName,
        event2Data.getID_Field_2,
        event2Data.getRequestedAt.toDateTime(DateTimeZone.forID("UTC")),
        event2Data.isIpAddressBlacklisted)
    }
  }

  case class event2Key(productCategoryType: String, ID_Field_2: String)
  object event2Key {
    def apply(event2Row:event2):event2Key = {
      new event2Key(
      event2Row.productCategoryType,
      event2Row.ID_Field_2
      )
    }
  }

  case class ConverterMaxDate(productCategoryType: String, ID_Field_2: String,  maxDate: DateTime)

}
