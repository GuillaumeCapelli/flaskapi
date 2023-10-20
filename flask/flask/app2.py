# Let's write your code here!
from flask import Flask, request, abort, json
from flask_basicauth import BasicAuth
import pymysql
import os
import math
from collections import defaultdict
from flask_swagger_ui import get_swaggerui_blueprint

app = Flask(__name__)
app.config.from_file("flask_config.json", load=json.load)
auth = BasicAuth(app)

MAX_PAGE_SIZE = 100


swaggerui_blueprint = get_swaggerui_blueprint(
    base_url='/docs',
    api_url='/static/openapi.yaml',
)
app.register_blueprint(swaggerui_blueprint)


def remove_null_fields(obj):
    return {k: v for k, v in obj.items() if v is not None}

@app.route("/")
def home():
    homepage = """
    <title>Hello welcome to my API</title>
    <body>
    <h1>HERE'S MY COMMANDS:</h1>
    <p>
    "/movies" "/movies/IDofthemovie" or "movies?page=1 or 2 or 10 or 200"
    </body>
    
    """
    return homepage

@app.route("/movies/<int:movie_id>")
@auth.required
def movie(movie_id):
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
            JOIN Bechdel B ON B.movieId = M.movieId 
            WHERE M.movieId=%s
        """, (movie_id,))
        movie = cursor.fetchone()

    if not movie:
        abort(404)

    movie = remove_null_fields(movie)

    with db_conn.cursor() as cursor:
        cursor.execute("""
            SELECT
                P.personId,
                P.primaryName AS name,
                P.birthYear,
                P.deathYear,
                MP.job,
                MP.category AS role
            FROM MoviesPeople MP
            JOIN People P on P.personId = MP.personId
            WHERE MP.movieId=%s
        """, (movie_id,))
        people = cursor.fetchall()

    movie['people'] = [remove_null_fields(p) for p in people]
    db_conn.close()
    return movie

@app.route("/movies")
@auth.required
def movies():
    page = int(request.args.get('page', 0))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)
    include_details = bool(int(request.args.get('include_details', 0)))
    
    db_conn = pymysql.connect(host="localhost", user="root", password=os.getenv("MySQL_password"), database="bechdel",
                              cursorclass=pymysql.cursors.DictCursor)
    
    with db_conn.cursor() as cursor:
        cursor.execute("""
            SELECT * FROM Movies
            ORDER BY movieId
            LIMIT %s
            OFFSET %s
        """, (page_size, page * page_size))       
        movies = cursor.fetchall()

    if include_details:
        movie_ids = [movie["movieId"] for movie in movies]
        movie_people = defaultdict(list)
        movie_genres = defaultdict(list)

        # Retrieve people
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
                JOIN People P on P.personId = MP.personId
                WHERE MP.movieId IN (%s)
            """ % ", ".join(["%s"] * len(movie_ids)), tuple(movie_ids))
            for person in cursor.fetchall():
                movie_people[person["movieId"]].append(remove_null_fields(person))

        # Retrieve genres
        with db_conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    MG.movieId,
                    G.genre
                FROM MoviesGenres MG
                JOIN Genres G ON G.genreId = MG.genreId
                WHERE MG.movieId IN (%s)
            """ % ", ".join(["%s"] * len(movie_ids)), tuple(movie_ids))
            for genre_record in cursor.fetchall():
                movie_genres[genre_record["movieId"]].append(genre_record["genre"])

        for movie in movies:
            movie["people"] = movie_people[movie["movieId"]]
            movie["genres"] = movie_genres[movie["movieId"]]

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
