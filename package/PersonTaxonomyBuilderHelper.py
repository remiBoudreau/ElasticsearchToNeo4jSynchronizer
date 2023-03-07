from pattern import Pattern
from node import Node
from nodeType import NodeType
from nodeRelationship import NodeRelationship
from nodeRelationshipType import NodeRelationshipType
from nodeConstraint import NodeConstraint
from relationshipConstraint import RelationshipConstraint
from taxonomy import Taxonomy
from nodeRelationshipMultiplicityConstraint import NodeRelationshipMultiplicityConstraint

class PersonTaxonomyBuilderHelper():
  def __init__(self):
    return

  def PERSON(self,name:str):
    p=Node(NodeType.PERSON)
    p.setAttribute("name",name)
    return p

  def ORGANIZATION(self,name:str):
    o=Node(NodeType.ORGANIZATION)
    o.setAttribute("name",name)
    return o

  def THING(self,name:str):
    t=Node(NodeType.THING)
    t.setAttribute("name",name)
    return t

  def DIGITALDOCUMENT(self,name:str):
    t=Node(NodeType.DIGITALDOCUMENT)
    t.setAttribute("name",name)
    return t

  def VULNERABILITY(self,name:str):
    t=Node(NodeType.VULNERABILITY)
    t.setAttribute("name",name)
    return t

  def PLACE(self,name:str):
    t=Node(NodeType.PLACE)
    t.setAttribute("name",name)
    return t

  def EMAIL(self,name:str):
    t=Node(NodeType.EMAIL)
    t.setAttribute("name",name)
    return t

  def PHONE(self,name:str):
    t=Node(NodeType.PHONE)
    t.setAttribute("name",name)
    return t

  def PASSPORT(self,name:str):
    t=Node(NodeType.PASSPORT)
    t.setAttribute("name",name)

    return t

  def SCHOOL(self,name:str):
    t=Node(NodeType.SCHOOL)
    t.setAttribute("name",name)
    return t

  def BANK_ACCOUNT(self,name:str):
    t=Node(NodeType.BANK_ACCOUNT)
    t.setAttribute("name",name)
    return t

  def PATENT(self,name:str):
    t=Node(NodeType.PATENT)
    t.setAttribute("name",name)
    return t

  def CERTIFICATION(self,name:str):
    t=Node(NodeType.CERTIFICATION)
    t.setAttribute("name",name)
    return t

  def PUBLISHED_WORK(self,name:str):
    t=Node(NodeType.PUBLISHED_WORK)
    t.setAttribute("name",name)
    return t

  def SOCIAL_SECURITY_NUMBER(self,name:str):
    t=Node(NodeType.SOCIAL_SECURITY_NUMBER)
    t.setAttribute("name",name)
    return t

  def SOCIAL_MEDIA(self,name:str):
    t=Node(NodeType.SOCIAL_MEDIA)
    t.setAttribute("name",name)
    return t
  
  def DATA_BREACH(self,name:str):
    t=Node(NodeType.DATA_BREACH)
    t.setAttribute("name",name)
    return t


  def OneExact(self,source:Node,dest:Node, rel):
    return NodeRelationship(rel,NodeRelationshipMultiplicityConstraint.ONE_TO_EXACTLY_ONE,source.getId(),source.getId(),None,None)

  def OnePlus(self,source:Node,dest:Node, rel):
    return NodeRelationship(rel,NodeRelationshipMultiplicityConstraint.ONE_TO_ONE_OR_MORE,source.getId(),dest.getId(),None,None)

  def ZeroPlus(self,source:Node,dest:Node, rel):

    return NodeRelationship(rel,NodeRelationshipMultiplicityConstraint.ONE_TO_ZERO_OR_MORE,source.getId(),dest.getId(),None,None)

  def createPersonTaxonomy(self,taxonomyName:str,persontaxonomy_json, startNode_id:str):
    node_fmapper={"Person":self.PERSON,
              "Place":self.PLACE,
              "Organization" : self.ORGANIZATION,
              "Email": self.EMAIL, 
              "Phone": self.PHONE, 
              "Passport": self.PASSPORT, 
              "School": self.SCHOOL, 
              "Bank_Account": self.BANK_ACCOUNT, 
              "Patent": self.PATENT, 
              "Certification": self.CERTIFICATION, 
              "Published_Work": self.PUBLISHED_WORK, 
              "Social_Security_Number": self.SOCIAL_SECURITY_NUMBER, 
              "Social_Media": self.SOCIAL_MEDIA,
              "Data_Breach": self.DATA_BREACH}
    
    
    nodeDict={}
    startNode = None
    for t in persontaxonomy_json:
        for side in ["Source","Destination"]:            
            nodeType = t[side]["NodeType"]
            nodeId= t[side]["id"]
            fToCall = node_fmapper[nodeType]
            if nodeType+nodeId not in nodeDict.keys():
                nodeDict[nodeType+nodeId] = fToCall(nodeType)

            if startNode is None:
                startNode= nodeDict[nodeType+nodeId]
        
        
    relList = []
    for t in persontaxonomy_json:#source,dest,rel, relType in thingRelationships:
        source =nodeDict[t["Source"]["NodeType"]+t["Source"]["id"]]
        dest   =nodeDict[t["Destination"]["NodeType"]+t["Destination"]["id"]]
        rel_type = t["Relationship"]["RelationshipType"]
        rel_mult = t["Relationship"]["RelationshipMultiplicity"]
        rel = NodeRelationshipType.value_of(rel_type)
        fRToCall = rel_mult
        aRel = fRToCall(source,dest, rel)
        relList.append(aRel)
    
    return Taxonomy(taxonomyName,startNode,[n for n in nodeDict.values()],relList,[],[])
