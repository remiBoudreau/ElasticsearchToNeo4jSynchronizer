from enum import Enum, auto

class NodeType(Enum): # look into schema.org and open-meta
  
  ORGANIZATION = "Organization"
  PERSON = "Person"
  THING = "Thing"
  PRODUCT = "Product"
  DIGITALDOCUMENT = "DigitalDocument"
  VULNERABILITY = "Vulnerability"
  PLACE = "Place"
  EMAIL = "Email"
  WEBSITE = "Website"
  PHONE = "Phone"
  PASSPORT = "Passport"
  SCHOOL = "School"
  BANK_ACCOUNT = "BankAccount"
  PATENT = "Patent"
  CERTIFICATION = "Certification"
  PUBLISHED_WORK = "PublishedWork"
  SOCIAL_SECURITY_NUMBER = "SocialSecurityNumber"
  SOCIAL_MEDIA = "SocialMedia"
  DATA_BREACH = "DataBreach"
  
  __schemaMap__ = {
    ORGANIZATION : "Organization",
    PERSON : "Person",
    THING : "Thing",
    PRODUCT : "Thing",
    DIGITALDOCUMENT : "Thing",
    VULNERABILITY : "Thing",
    PLACE : "Thing",
    EMAIL : "Thing",
    WEBSITE : "Thing",
    PHONE : "Thing",
    PASSPORT : "Thing",
    SCHOOL : "Organization",
    BANK_ACCOUNT : "Thing",
    PATENT : "Thing",
    CERTIFICATION : "Thing",
    PUBLISHED_WORK : "Thing",
    SOCIAL_SECURITY_NUMBER : "Thing",
    SOCIAL_MEDIA : "Thing",
    DATA_BREACH : "Thing" ,
  }
  
  def schema(self):
    return self.__schemaMap__[self._value_]
  


