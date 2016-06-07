#ifndef __FLOPPY_TABLE_ATTRIBUTE_H__
#define __FLOPPY_TABLE_ATTRIBUTE_H__

class FLOPPYTableAttribute {
   public:
      FLOPPYTableAttribute();
      virtual ~FLOPPYTableAttribute();

      char *tableName;
      char *attribute;
};

#endif /* __FLOPPY_TABLE_ATTRIBUTE_H__ */
