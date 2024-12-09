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
