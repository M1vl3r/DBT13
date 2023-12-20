import mysql.connector

def execute_query(connection, query, params=None):
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query, params)
    result = cursor.fetchall()
    cursor.close()
    return result

def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='107.0.0.1',
            port=3306,
            database='dbt13',
            user='root'
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

# Задание 1: Количество музыкальных произведений заданного ансамбля
def count_works_in_ensemble(connection, ensemble_name):
    query = '''
        SELECT COUNT(*)
        FROM MusicalWorks M
        JOIN Ensembles E ON M.EnsembleID = E.EnsembleID
        WHERE E.EnsembleName = %s
    '''
    params = (ensemble_name,)
    result = execute_query(connection, query, params)
    print(f"Количество музыкальных произведений для ансамбля {ensemble_name}:")
    print(result[0]['COUNT(*)'])

# Задание 2: Название всех компакт-дисков заданного ансамбля
def list_cd_titles_by_ensemble(connection, ensemble_name):
    query = '''
        SELECT DISTINCT R.RecordID, R.ReleaseDate, R.Company, R.WholesalePrice, R.RetailPrice
        FROM Performances P
        JOIN Records R ON P.RecordID = R.RecordID
        JOIN MusicalWorks M ON P.WorkID = M.WorkID
        JOIN Ensembles E ON M.EnsembleID = E.EnsembleID
        WHERE E.EnsembleName = %s
    '''
    params = (ensemble_name,)
    result = execute_query(connection, query, params)
    print(f"Название всех компакт-дисков для ансамбля {ensemble_name}:")
    print(result)

# Задание 3: Показать лидеров продаж текущего года
def list_best_sellers(connection):
    query = '''
        SELECT R.RecordID, R.ReleaseDate, R.Company, R.WholesalePrice, R.RetailPrice,
               SUM(R.SoldThisYear) AS TotalSales
        FROM Records R
        GROUP BY R.RecordID, R.ReleaseDate, R.Company, R.WholesalePrice, R.RetailPrice
        ORDER BY TotalSales DESC
    '''
    result = execute_query(connection, query)
    print("Лидеры продаж текущего года:")
    print(result)

# Задание 4: Предусмотреть изменения данных о компакт-дисках и ввод новых данных
def update_record_info(connection, record_id, new_wholesale_price, new_retail_price):
    query = '''
        UPDATE Records
        SET WholesalePrice = %s, RetailPrice = %s
        WHERE RecordID = %s
    '''
    params = (new_wholesale_price, new_retail_price, record_id)
    execute_query(connection, query, params)
    print(f"Данные о компакт-диске с RecordID {record_id} обновлены.")

# Задание 5: Предусмотреть ввод новых данных об ансамблях
def add_new_ensemble(connection, ensemble_name, leader_id):
    query = '''
        INSERT INTO Ensembles (EnsembleName, LeaderID)
        VALUES (%s, %s)
    '''
    params = (ensemble_name, leader_id)
    execute_query(connection, query, params)
    print(f"Новый ансамбль {ensemble_name} добавлен.")

# Подключаемся к базе данных
connection = connect_to_database()

if not connection:
    print("Не удалось подключиться к базе данных.")
else:
    try:
        # Задание 1
        ensemble_name = 'Symphony Orchestra'
        count_works_in_ensemble(connection, ensemble_name)

        # Задание 2
        list_cd_titles_by_ensemble(connection, ensemble_name)

        # Задание 3
        list_best_sellers(connection)

        # Задание 4
        record_id_to_update = 1
        new_wholesale_price = 15.99
        new_retail_price = 24.99
        update_record_info(connection, record_id_to_update, new_wholesale_price, new_retail_price)

        # Задание 5
        new_ensemble_name = 'New Jazz Quartet'
        leader_musician_id = 3  # Assuming the leader's MusicianID
        add_new_ensemble(connection, new_ensemble_name, leader_musician_id)

    except Exception as e:
        print(f"Error: {e}")

    # Закрываем соединение с базой данных
    connection.close()
