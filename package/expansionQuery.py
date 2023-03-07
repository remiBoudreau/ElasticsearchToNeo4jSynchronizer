#!/usr/bin/env python3
from pattern import Pattern

class ExpansionQuery(Pattern):
  def __init__(self,
               searchId: str,
               taxonomyId: str,
               cloudEventPayload:[dict]):
    Pattern.__init__(self)
    ce = [{
            "key":"search-id",
            "value":f"{searchId}",
            "subject":"Search",
        },
        {
            "key":"taxonomy-id",
            "value":f"{taxonomyId}",
            "subject":"Taxonomy",
        },
        {
            "key":"expansion-query-id",
            "value":f"{self.getId()}",
            "subject":"ExpansionQuery",
        }
    ]
    
    for item in cloudEventPayload:
        ce.append(item)
    self.cloudEventPayload=ce

  def getCloudEventPayload(self):
    return self.cloudEventPayload