import mysql.connector
from mysql.connector import errorcode

def create_connection():

    try:
        connection = mysql.connector.connect(
            user= "movies_user",
            password= "popcorn",
            host= "localhost",
            database= "movies",
            raise_on_warnings= True
        )
        if connection.is_connected():
            print("Connected to the database.")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

def execute_query(connection, query):
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"Error executing query: {e}")
        return None

def main():
    connection = create_connection()

    if not connection:
        return

    #query 1: Select all fields for the studio table
    print("\n1. Query to select all fields for the studio table.")
    query1 = "SELECT FROM studio;"
    result1 = execute_query(connection, query1)
    print("Studio Table Data:")
    for row in result1:
        print(row)

    #Query 2: Select all fields for the genre table
    print("\n2. Query to select all fields for the movies table.")
    query2 = "SELECT FROM genre;"
    result2 = execute_query(connection, query2)
    print("Genre Table Data:")
    for row in result2:
        print(row)

    #Query 3: Select movie names for movies with a runtime of less than 2 hours
    print("\n3. Query to select movie names for the movies with a runtime of less than 2 hours.")
    query3 = "SELECT film_name FROM film WHERE film_runtime < 120;"
    result3 = execute_query(connection, query3)
    print("Films with a runtime of less than 2 hours:")
    for row in result3:
        print(f"-{row['film_name']}")

    #Query 4: Get a list of film names and directors grouped by director
    print("\n4. Query to get a list of film names and directors grouped by director.")
    query4 = """
        SELECT film_director, 
                GROUP_CONCAT(film_name SEPARATOR ', ') AS films
        FROM film
        GROUP BY film_director;
    """
    result4 = execute_query(connection, query4)
    print("Films grouped by director:")
    for row in result4:
        print(f"-{row['director']}: {row['film']}")

    #close the connection
    connection.close()
    print("\nConnection closed.")

if __name__ == "__main__":
    main()