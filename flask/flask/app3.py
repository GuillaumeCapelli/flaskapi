from collections import defaultdict
from flask import Flask, request, abort, json
from flask_basicauth import BasicAuth
import pymysql
import math
import os

app = Flask(__name__)
app.config.from_file("flask_config.json", load=json.load)
auth = BasicAuth(app)

MAX_PAGE_SIZE = 100

@app.route("/movies")
@auth.required
def movies():
    page = int(request.args.get('page', 0))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)
    include_details = int(request.args.get('include_details', 0))

    db_conn = pymysql.connect(host="localhost", user="root", password=os.getenv("MySQL_password"), database="bechdel",
                              cursorclass=pymysql.cursors.DictCursor)

    with db_conn.cursor() as cursor:
        cursor.execute("""
            SELECT
                M.movieId,
                M.originalTitle,
                M.primaryTitle AS englishTitle,
                B.rating AS bechdelScore,
                M.runtimeMinutes,
                M.startYear AS year,
                M.movieType,
                M.isAdult
            FROM Movies M
            LEFT JOIN Bechdel B ON B.movieId = M.movieId
            ORDER BY M.movieId
            LIMIT %s
            OFFSET %s
        """, (page_size, page * page_size))

        movies = cursor.fetchall()

    if include_details:
        movie_ids = [movie['movieId'] for movie in movies]

        with db_conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    MP.movieId,
                    P.personId,
                    P.primaryName AS name,
                    P.birthYear,
                    P.deathYear,
                    MP.job,
                    MP.category AS role
                FROM MoviesPeople MP
                JOIN People P ON P.personId = MP.personId
                WHERE MP.movieId IN (%s)
            """ % ", ".join(['%s'] * len(movie_ids)), tuple(movie_ids))

            people = cursor.fetchall()

        people_grouped_by_movie = defaultdict(list)
        for person in people:
            people_grouped_by_movie[person['movieId']].append(person)

        # Assume there's a Genres table in your database. You can adjust this part if needed.
        with db_conn.cursor() as cursor:
            cursor.execute("""
                SELECT movieId, genre
                FROM Genres
                WHERE movieId IN (%s)
            """ % ", ".join(['%s'] * len(movie_ids)), tuple(movie_ids))

            genres = cursor.fetchall()

        genres_grouped_by_movie = defaultdict(list)
        for genre in genres:
            genres_grouped_by_movie[genre['movieId']].append(genre['genre'])

        for movie in movies:
            movie['people'] = people_grouped_by_movie.get(movie['movieId'], [])
            movie['genres'] = genres_grouped_by_movie.get(movie['movieId'], [])

    with db_conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS total FROM Movies")
        total = cursor.fetchone()
        last_page = math.ceil(total['total'] / page_size)

    db_conn.close()

    return {
        'movies': movies,
        'next_page': f'/movies?page={page+1}&page_size={page_size}',
        'last_page': f'/movies?page={last_page}&page_size={page_size}',
    }


if __name__ == '__main__':
    app.run(debug=True, port=8080)

