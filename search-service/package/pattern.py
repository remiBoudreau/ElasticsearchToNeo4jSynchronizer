import uuid

# Inspiration from: https://stackoverflow.com/questions/69125694/dynamically-add-overwrite-the-setter-and-getter-of-property-attributes
class Pattern:
  def __init__(self):
    self.id = f'n{uuid.uuid4().hex}'

  def getId(self):
      return self.id
