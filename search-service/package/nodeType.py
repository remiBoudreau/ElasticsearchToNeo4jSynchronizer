from enum import Enum, auto

class NodeType(Enum): # look into schema.org and open-meta
  ORGANIZATION = "Organization"
  PERSON = "Person"
  THING = "Thing"
  DIGITALDOCUMENT = "DigitalDocument"
  VULNERABILITY = "Vulnerability"
  PLACE = "Place"
  EMAIL = "Thing"
  PHONE = "Thing"
  PASSPORT = "DigitalDocument"
  SCHOOL = "School"
  BANK_ACCOUNT = "Thing"
  PATENT = "DigitalDocument"
  CERTIFICATION = "DigitalDocument"
  PUBLISHED_WORK = "Thing"
  SOCIAL_SECURITY_NUMBER = "DigitalDocument"
  SOCIAL_MEDIA = "Thing"
  DATA_BREACH = "Thing"
  



