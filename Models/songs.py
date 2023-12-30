from Models.Model import Model

class songs(Model):
    __nameTable = 'songs'
    __name_of_song = 'name_of_song'
    __year = 'year'
    __ensemble = 'ensemble'

    def get(self):
        return super().get(self.__nameTable)

    def getOneField(self, field):
        return super().getOneField(self.__nameTable,field)

    def delete(self,id):
        super().delete(self.__nameTable,id)

    def update(self,table,field,id,values):
        super().update(self.__nameTable, field, values, id)

    def getOneRow(self, id):
        return super().getOneRow(self.__nameTable,id)