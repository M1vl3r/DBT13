from Models.Model import Model

class disc(Model):
    __nameTable = 'disc'
    __name_of_disc = 'name_of_disc'
    __year = 'year'
    __ensemble = 'ensemble'
    __song1 = 'song1'
    __song2 = 'song2'

    def get(self):
        return super().get(self.__nameTable)

    def getOneField(self, field):
        return super().getOneField(self.__nameTable,field)

    def delete(self,id):
        super().delete(self.__nameTable,id)

    def update(self,table,field,id,values):
        super().update(self.__nameTable, field, values, id)

    def getOneRow(self, id):
        return super().getOneRow(self.__nameTable, id)

    def add(self):
        name_of_disc = input("Введите название диска: ")
        year = input("Введите год выпуска в формате ГГГГ-ММ-ДД: ")
        ensemble = input("Введите название ансамбля который выпустил данный диск: ")
        song1 = input("Введите название песни: ")
        song2 = input("Введите название песни:")
        str = f"{self.__name_of_disc}, {self.__year},{self.__ensemble},{self.__song1},{self.__song2}"
        super().add(self.__nameTable, str, name_of_disc, year, ensemble, song1, song2)
