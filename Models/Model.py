from Configuration.config import connection

class Model:

    def get(self,table):
        with connection().cursor() as cursor:
            select_all_rows = f"SELECT * FROM {table}"
            cursor.execute(select_all_rows)
            return cursor.fetchall()
        connection().close()

    def getOneField(self,table, field):
        with connection().cursor() as cursor:
            select_one_field = f"SELECT {field} FROM {table}"
            cursor.execute(select_one_field)
            return cursor.fetchall()
        connection().close()

    def add(self, table,str, *values):
        with connection().cursor() as cursor:
            print( f"INSERT INTO {table} ({str}) VALUES {values}")
            query = f"INSERT INTO {table} ({str}) VALUES {values}"
            cursor.execute(query)
        connection().close()
        print(f"Новая запись в таблицу {table} добавлена")

    def delete(self,table, id):
        with connection().cursor() as cursor:
            query_delete = f"DELETE FROM {table} WHERE id = {id}"
            cursor.execute(query_delete)
            connection().commit()
        connection().close()
        print("Запись удалена")

    def update(self,table,field,values,id):
        with connection().cursor() as cursor:
            query_update = f"UPDATE {table} SET {field} ='{values}' WHERE id = {id}"
            cursor.execute(query_update)
            connection().commit()
        connection().close()
        print("Запись добавлена")

    def getOneRow(self, table, id):
        with connection().cursor() as cursor:
            query = f"SELECT * FROM {table} WHERE id = {id}"
            cursor.execute(query)
            return cursor.fetchall()
        connection().close()