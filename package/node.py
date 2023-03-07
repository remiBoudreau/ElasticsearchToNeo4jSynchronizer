#!/usr/bin/env python3

from pattern import Pattern
from nodeType import NodeType
from schemaorg.main import Schema

class Node(Pattern):
  def __init__(self,nodeType: NodeType):
    Pattern.__init__(self)
    # From schema.org get the list of fields
    self.attributes={}

    self.attributes["NodeType"]=nodeType.value
    
    # sys.stdout = open(os.devnull, "w")
    # sys.stderr = open(os.devnull, "w")
    try:
        spec = Schema(nodeType.value)
    # sys.stdout = sys.__stdout__
    # sys.stderr = sys.__stderr__
    # add fields
    except:
        print("We are unable to create this type. Creating nodetype as 'THING'")
        spec = Schema(NodeType.THING.value)
        

        
    for name, meta in spec._properties.items():
      self.attributes[name]=meta

  def getAttributesList(self):
    return self.attributes.keys()

  def getAttribute(self,name):
    if name in self.attributes.keys():
      return self.attributes[name]
    else:
      return "Buddy. Yo... That's not a thing bro!"
  
  def setAttribute(self,name,value):
    if name in self.attributes.keys():
      self.attributes[name]=value
      return True
    else:
      return "Buddy. Yo... That's not a thing bro!"
