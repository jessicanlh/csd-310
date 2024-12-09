def show_films(cursor, title):

    # Print the title
    print("\n" + title)
    print("=" * len(title))

    # SQL query to select required fields using INNER JOINs
    query = (
        "SELECT film.film_name AS Name, "
        "       film.film_director AS Director, "
        "       genre.genre_name AS Genre, "
        "       studio.studio_name AS Studio "
        "FROM film "
        "INNER JOIN genre ON film.genre_id = genre.genre_id "
        "INNER JOIN studio ON film.studio_id = studio.studio_id"
    )

    # Execute the query
    cursor.execute(query)

    # Fetch all results
    results = cursor.fetchall()

    # Format the output labels
    print(f"{'Name':<30} {'Director':<30} {'Genre':<15} {'Studio':<25}")
    print("-" * 100)

    # Iterate over the data set and display results
    for row in results:
        name, director, genre, studio = row
        print(f"{name:<30} {director:<30} {genre:<15} {studio:<25}")

# Example usage (assuming a valid database connection and cursor):
import mysql.connector
connection = mysql.connector.connect(user='root', password='Copper!12', host='localhost', database='movies')
cursor = connection.cursor()

# Initial display
display_films(cursor, "DISPLAYING FILMS")

# Insert a new record
insert_query = (
    "INSERT INTO film (film_name, film_releaseDate, film_runtime, film_director, studio_id, genre_id) "
    "VALUES ('Inception', '2010', 148, 'Christopher Nolan', "
    "(SELECT studio_id FROM studio WHERE studio_name = 'Universal Pictures'), "
    "(SELECT genre_id FROM genre WHERE genre_name = 'SciFi'))"
)
cursor.execute(insert_query)
connection.commit()

# Display after insertion
show_films(cursor, "DISPLAYING FILMS AFTER INSERTION")

# Update Alien to Horror
update_query = (
    "UPDATE film "
    "SET genre_id = (SELECT genre_id FROM genre WHERE genre_name = 'Horror') "
    "WHERE film_name = 'Alien'"
)
cursor.execute(update_query)
connection.commit()

# Display after update
show_films(cursor, "DISPLAYING FILMS AFTER UPDATE")

# Delete Gladiator
delete_query = "DELETE FROM film WHERE film_name = 'Gladiator'"
cursor.execute(delete_query)
connection.commit()

# Display after deletion
show_films(cursor, "DISPLAYING FILMS AFTER DELETION")

# Close cursor and connection
cursor.close()
connection.close()
