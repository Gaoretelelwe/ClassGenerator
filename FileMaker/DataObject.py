import os

class DataObject():

    def __init__(self, Name = None, Columns = None, Destination = None):
        self.Name = Name 
        self.Columns = Columns 
        self.Destination = Destination

        path = Destination + "/" + Name + ".py"
        ObjectFile = open(path, "w") 

        if Name == "Session":
            self.GenerateSessionFile(ObjectFile)
        else:
            self.GenerateGenericFile(ObjectFile)

        ObjectFile.close()


    def GenerateSessionFile(self, ObjectFile):
        self.WriteHeader(ObjectFile)
        self.WriteInitMethod(ObjectFile)
        self.WriteSaveMethod(ObjectFile)
        self.WriteDBFetch(ObjectFile)
        self.WriteDBFetchGuid(ObjectFile)
        self.WriteDeleteMethod(ObjectFile)
        self.WriteDBInsertCheckMethod(ObjectFile)
        self.WriteDBInsertMethod(ObjectFile)
        self.WriteDBFetchCheckMethod(ObjectFile)
        self.WriteDBFetchMethod(ObjectFile)
        self.WriteDBFetchGuidCheckMethod(ObjectFile)
        self.WriteDBFetchGuidMethod(ObjectFile)
        self.WriteDBDeleteCheckMethod(ObjectFile)
        self.WriteDBDeleteMethod(ObjectFile)
        self.WriteDBUpdateCheckMethod(ObjectFile)
        self.WriteDBUpdateMethod(ObjectFile)


    def GenerateGenericFile(self, ObjectFile):
        self.WriteHeader(ObjectFile)
        self.WriteInitMethod(ObjectFile)
        self.WriteSaveMethod(ObjectFile)
        self.WriteDBFetch(ObjectFile)
        self.WriteDeleteMethod(ObjectFile)
        self.WriteDBInsertCheckMethod(ObjectFile)
        self.WriteDBInsertMethod(ObjectFile)
        self.WriteDBFetchCheckMethod(ObjectFile)
        self.WriteDBFetchMethod(ObjectFile)
        self.WriteDBDeleteCheckMethod(ObjectFile)
        self.WriteDBDeleteMethod(ObjectFile)
        self.WriteDBUpdateCheckMethod(ObjectFile)
        self.WriteDBUpdateMethod(ObjectFile)


    def WriteHeader(self, ObjectFile):
        ObjectFile.write("from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData\n\n")
        ObjectFile.write("from DataAccess.DataAccess import DataAccess\n")
        ObjectFile.write("from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError\n\n")
        ObjectFile.write("class " + self.Name + "():\n\n")

    def WriteInitMethod(self, ObjectFile):
        ObjectFile.write("\tdef __init__(self, ")

        for column in self.Columns:
            ObjectFile.write(column[0] + " = None, ")

        ObjectFile.write("data_access = None):\n")

        ObjectFile.write("\t\tself.meta = MetaData()\n")
        ObjectFile.write("\t\tself.data_access = data_access\n\n")
        
        ObjectFile.write("\t\tself.IsFetched = False\n")
        ObjectFile.write("\t\tself.IsInserted = False\n")
        ObjectFile.write("\t\tself.IsUpdated = False\n")
        ObjectFile.write("\t\tself.IsDeleted = False\n\n")

        ObjectFile.write("\t\tself.content = Table(\n")
        ObjectFile.write("\t\t\t'" + self.Name + "', self.meta,\n")

        for column in self.Columns:
            columnName = column[0]
            columnType = self.GetColumnType(column[1])
            columnKey = column[3]

            if columnKey == "PRI":
                ObjectFile.write("\t\t\tColumn('" + columnName + "', " + columnType + ", primary_key = True),\n")
            elif columnKey == "UNI":
                ObjectFile.write("\t\t\tColumn('" + columnName + "', " + columnType + ", unique = True),\n")
            else:
                ObjectFile.write("\t\t\tColumn('" + columnName + "', " + columnType + "),\n")

        ObjectFile.write("\t\t)\n\n")
        
        for column in self.Columns:
            columnName = column[0]
            ObjectFile.write("\t\tself." + columnName + " = " + columnName + "\n")
        
        ObjectFile.write("\n")

    def WriteSaveMethod(self, ObjectFile):
        ObjectFile.write("\tdef Save(self):\n")
        
        ObjectFile.write("\t\ttry:\n")
        ObjectFile.write("\t\t\tif not self.IsInserted and not self.IsFetched:\n")
        ObjectFile.write("\t\t\t\tself._db_insert_check()\n")
        ObjectFile.write("\t\t\t\tself._db_insert()\n")
        ObjectFile.write("\t\t\telse:\n")
        ObjectFile.write("\t\t\t\tself._db_update_check()\n")
        ObjectFile.write("\t\t\t\tself._db_update()\n")
        ObjectFile.write("\t\texcept InsertError as insert_error:\n")
        ObjectFile.write("\t\t\traise insert_error\n")

        ObjectFile.write("\n")

    def WriteDBFetch(self, ObjectFile):
        ObjectFile.write("\tdef DBFetch(self, " + self.Name + "Id):\n")
        
        ObjectFile.write("\t\ttry:\n")
        ObjectFile.write("\t\t\tif not self.IsInserted and not self.IsFetched:\n")
        ObjectFile.write("\t\t\t\tself._db_fetch_check()\n")
        ObjectFile.write("\t\t\t\tself._db_fetch(" + self.Name + "Id)\n")
        ObjectFile.write("\t\texcept FetchError as fetch_error:\n")
        ObjectFile.write("\t\t\traise fetch_error\n")

        ObjectFile.write("\n")

    def WriteDBFetchGuid(self, ObjectFile):
        ObjectFile.write("\tdef DBFetchGuid(self, " + self.Name + "Guid):\n")
        
        ObjectFile.write("\t\ttry:\n")
        ObjectFile.write("\t\t\tif not self.IsInserted and not self.IsFetched:\n")
        ObjectFile.write("\t\t\t\tself._db_fetch_guid_check()\n")
        ObjectFile.write("\t\t\t\tself._db_fetch_guid(" + self.Name + "Guid)\n")
        ObjectFile.write("\t\texcept FetchError as fetch_error:\n")
        ObjectFile.write("\t\t\traise fetch_error\n")

        ObjectFile.write("\n")

    def WriteDeleteMethod(self, ObjectFile):
        ObjectFile.write("\tdef Delete(self):\n")
        
        ObjectFile.write("\t\ttry:\n")
        ObjectFile.write("\t\t\tif self.IsInserted or self.IsFetched:\n")
        ObjectFile.write("\t\t\t\tself._db_delete_check()\n")
        ObjectFile.write("\t\t\t\tself._db_delete()\n")
        ObjectFile.write("\t\t\telse:\n")
        ObjectFile.write("\t\t\t\traise DeleteError('The " + self.Name + " is neither fetched nor inserted.')\n")
        ObjectFile.write("\t\texcept DeleteError as delete_error:\n")
        ObjectFile.write("\t\t\traise delete_error\n")

        ObjectFile.write("\n")

    def WriteDBInsertCheckMethod(self, ObjectFile):
        ObjectFile.write("\tdef _db_insert_check(self):\n")

        ObjectFile.write("\t\tif self.IsDeleted:\n")
        ObjectFile.write("\t\t\traise InsertError('The " + self.Name + " is deleted.')\n")
        ObjectFile.write("\n")

        ObjectFile.write("\t\tif self.IsInserted:\n")
        ObjectFile.write("\t\t\traise InsertError('The " + self.Name + " is inserted.')\n")
        ObjectFile.write("\n")

        ObjectFile.write("\t\tif self.IsUpdated:\n")
        ObjectFile.write("\t\t\traise InsertError('The " + self.Name + " is updated.')\n")
        ObjectFile.write("\n")

        for column in self.Columns:
            columnName = column[0]
            columnKey = column[3]
            isNullable = column[4]
            if isNullable == "NO" and columnKey != "PRI":
                ObjectFile.write("\t\tif self." + columnName + " == None:\n")
                ObjectFile.write("\t\t\traise InsertError('Please make sure that " + columnName + " has a value.')\n")
                ObjectFile.write("\n")

    def WriteDBInsertMethod(self, ObjectFile):
        ObjectFile.write("\tdef _db_insert(self):\n")

        ObjectFile.write("\t\tif not self.IsInserted and not self.IsFetched and not self.IsUpdated:\n")

        ObjectFile.write("\t\t\tresult = self.data_access.connection.execute(self.content.insert(), [\n")

        ObjectFile.write("\t\t\t\t{")

        countColumns = len(self.Columns) - 1
        counter = 0

        for column in self.Columns:
            columnName = column[0]
            columnKey = column[3]
            
            if columnKey != "PRI":
                if counter < countColumns:
                    ObjectFile.write("'" + columnName + "':self." + columnName + ", ")
                else:
                    ObjectFile.write("'" + columnName + "':self." + columnName)
            
            counter = counter + 1

        ObjectFile.write("},\n")

        ObjectFile.write("\t\t\t])\n")

        ObjectFile.write("\t\t\tself." + self.Name + "Id = result.inserted_primary_key\n")
        ObjectFile.write("\t\t\tself.IsInserted = True\n")
        ObjectFile.write("\t\t\tself.IsFetched = True\n")

        ObjectFile.write("\n")

    def WriteDBFetchCheckMethod(self, ObjectFile):
        ObjectFile.write("\tdef _db_fetch_check(self):\n")

        ObjectFile.write("\t\tif self.IsDeleted:\n")
        ObjectFile.write("\t\t\traise FetchError('The " + self.Name + " is deleted.')\n")

        ObjectFile.write("\n")

    def WriteDBFetchGuidCheckMethod(self, ObjectFile):
        ObjectFile.write("\tdef _db_fetch_guid_check(self):\n")

        ObjectFile.write("\t\tif self.IsDeleted:\n")
        ObjectFile.write("\t\t\traise FetchError('The " + self.Name + " is deleted.')\n")

        ObjectFile.write("\n")

    def WriteDBFetchMethod(self, ObjectFile):
        ObjectFile.write("\tdef _db_fetch(self, " + self.Name + "Id):\n")
        
        ObjectFile.write("\t\tif not self.IsFetched:\n")

        ObjectFile.write("\t\t\ts = self.content.select().where(self.content.c.")
        ObjectFile.write(self.Name + "Id == self." + self.Name + "Id)\n")

        ObjectFile.write("\t\t\tresult = self.data_access.connection.execute(s)\n")
        ObjectFile.write("\t\t\trow = result.first()\n\n")

        ObjectFile.write("\t\t\tif row == None:\n")
        ObjectFile.write("\t\t\t\traise FetchError('The " + self.Name + " does not exist. " + self.Name + " Id is {0}.'.format(str(" + self.Name + "Id)))\n")
        ObjectFile.write("\t\t\telse:\n")
        ObjectFile.write("\t\t\t\t#Get results and assign them to class variables\n\n")

        countColumns = 0

        for column in self.Columns:
            columnName = column[0]
            ObjectFile.write("\t\t\t\tself." + columnName + " = row[" + str(countColumns) + "]\n")
            countColumns = countColumns + 1

        ObjectFile.write("\t\t\t\tself.IsFetched = True\n")

        ObjectFile.write("\n")

    def WriteDBFetchGuidMethod(self, ObjectFile):
        ObjectFile.write("\tdef _db_fetch_guid(self, " + self.Name + "Guid):\n")
        
        ObjectFile.write("\t\tif not self.IsFetched:\n")

        ObjectFile.write("\t\t\ts = self.content.select().where(self.content.c.")
        ObjectFile.write(self.Name + "Guid == self." + self.Name + "Guid)\n")

        ObjectFile.write("\t\t\tresult = self.data_access.connection.execute(s)\n")
        ObjectFile.write("\t\t\trow = result.first()\n\n")

        ObjectFile.write("\t\t\tif row == None:\n")
        ObjectFile.write("\t\t\t\traise FetchError('The " + self.Name + " does not exist. " + self.Name + " Guid is {0}.'.format(str(" + self.Name + "Guid)))\n")
        ObjectFile.write("\t\t\telse:\n")
        ObjectFile.write("\t\t\t\t#Get results and assign them to class variables\n\n")

        countColumns = 0

        for column in self.Columns:
            columnName = column[0]
            ObjectFile.write("\t\t\t\tself." + columnName + " = row[" + str(countColumns) + "]\n")
            countColumns = countColumns + 1

        ObjectFile.write("\t\t\t\tself.IsFetched = True\n")

        ObjectFile.write("\n")

    def WriteDBDeleteCheckMethod(self, ObjectFile):
        ObjectFile.write("\tdef _db_delete_check(self):\n")

        ObjectFile.write("\t\tif self.IsDeleted:\n")
        ObjectFile.write("\t\t\traise DeleteError('The " + self.Name + " is deleted.')\n")

        ObjectFile.write("\n")

    def WriteDBDeleteMethod(self, ObjectFile):
        ObjectFile.write("\tdef _db_delete(self):\n")
        
        ObjectFile.write("\t\ts = self.content.delete().where(self.content.c.")
        ObjectFile.write(self.Name + "Id == self." + self.Name + "Id)\n")

        ObjectFile.write("\t\tself.data_access.connection.execute(s)\n\n")

        ObjectFile.write("\t\tself.IsDeleted = True\n")

        ObjectFile.write("\n")

    def WriteDBUpdateCheckMethod(self, ObjectFile):
        ObjectFile.write("\tdef _db_update_check(self):\n")

        ObjectFile.write("\t\tif self.IsDeleted:\n")
        ObjectFile.write("\t\t\traise UpdateError('The " + self.Name + " is deleted.')\n")

        ObjectFile.write("\n")

    def WriteDBUpdateMethod(self, ObjectFile):
        ObjectFile.write("\tdef _db_update(self):\n")
        
        ObjectFile.write("\t\ts = self.content.update().where(self.content.c.")
        ObjectFile.write(self.Name + "Id == self.")
        ObjectFile.write(self.Name + "Id).values(")

        countColumns = len(self.Columns) - 1
        counter = 0

        for column in self.Columns:
            columnName = column[0]
            
            if columnName != self.Name + "Id":
                if counter < countColumns:
                    ObjectFile.write(columnName + " = self." + columnName + ", ")
                else:
                    ObjectFile.write(columnName + " = self." + columnName)
            
            counter = counter + 1

        ObjectFile.write(")\n")

        ObjectFile.write("\t\tself.data_access.connection.execute(s)\n\n")

        ObjectFile.write("\t\tself.IsUpdated = True\n")

        ObjectFile.write("\n")
    
    def GetColumnType(self, DataType = None):

        if DataType == "int":
            return "Integer"
        elif DataType == "varchar":
            return "String"
        elif DataType in ("datetime", "date"):
            return "DateTime"
        elif DataType == "decimal":
            return "Float"
        else:
            return "String"
        

