from Models.Model import Model

class ensemble(Model):
    __nameTable = 'ensemble'
    __name_of_ensemble = 'name_of_ensemble'
    __type_of_ensemble = 'type_of_ensemble'
    __musicians1 = 'musicians1'
    __musicians2 = 'musicians2'
    __musicians3 = 'musicians2'

    def get(self):
        return super().get(self.__nameTable)

    def getOneField(self, field):
        return super().getOneField(self.__nameTable,field)

    def delete(self,id):
        super().delete(self.__nameTable,id)

    def update(self,table,field,id,values):
        super().update(self.__nameTable, field, values, id)

    def add(self):
        name_of_ensemble = input("Введите название ансамбля: ")
        __type_of_ensemble = input("Введите тип ансамбля: ")
        __musicians1 = input("Введите первого музыканта состоящего в ансамбле: ")
        __musicians2 = input("Введите второго музыканта состоящего в ансамбле: ")
        __musicians3 = input("Введите третьего музыканта состоящего в ансамбле: ")
        str = f"{self.__name_of_ensemble}, {self.__type_of_ensemble},{self.__musicians1},{self.__musicians2},{self.__musicians3}"
        super().add(self.__nameTable, str, name_of_ensemble,__type_of_ensemble,__musicians1,__musicians2,__musicians3)
