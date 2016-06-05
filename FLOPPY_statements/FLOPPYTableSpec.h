#ifndef __FLOPPY_TABLE_SPEC_H__
#define __FLOPPY_TABLE_SPEC_H__

class FLOPPYTableSpec {
   public:
      FLOPPYTableSpec();
      virtual ~FLOPPYTableSpec();

      char *tableName;
      char *alias;
};

#endif /* __FLOPPY_TABLE_SPEC_H__ */
