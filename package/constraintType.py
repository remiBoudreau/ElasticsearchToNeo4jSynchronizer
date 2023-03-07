from enum import Enum
class ConstraintType(Enum):
  STARTSWITH = "startsWith"
  ENDSWITH = "endsWith"
  EQUALS = "equals"
  DIFFERENT = "different"
  GREATERTHAN = "greater_than"
  LESSTHAN = "less_than"
  GREATEROREQUALTHAN = "greater_or_equal_than"
  LESSOREQUALTHAN = "less_or_equal_than"
  CONTAINS = "contains"
  REGEX = "regex"

  def getNeo4jName(self):
    if self.name==self.STARTSWITH.name:
      return " STARTS WITH "
    if self.name==self.ENDSWITH.name:
      return " ENDS WITH "
    if self.name==self.EQUALS.name:
      return " = "
    if self.name==self.DIFFERENT.name:
      return " <> "
    if self.name==self.GREATERTHAN.name:
      return " > "
    if self.name==self.LESSTHAN.name:
      return " < "
    if self.name==self.GREATEROREQUALTHAN.name:
      return " >= "
    if self.name==self.LESSOREQUALTHAN.name:
      return " <= "
    if self.name==self.LESSOREQUALTHAN.name:
      return " CONTAINS "
    if self.name==self.REGEX.name:
      return " =~ "
#     if self.name==self.THING.name:
#       return "Thing"
#     if self.name==self.PERSON.name:
#       return "Person"
#     if self.name==self.ORGANIZATION.name:
#       return "Organization"