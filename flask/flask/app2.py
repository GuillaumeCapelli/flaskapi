# Let's write your code here!
from flask import Flask
import pymysql

app = Flask(__name__)

@app.route("/movies/<int:movie_id>")
def movie(movie_id):
    db_conn = pymysql.connect(host="localhost", user="root", password="Arsenal75013", database="bechdel",
                              cursorclass=pymysql.cursors.DictCursor)
    with db_conn.cursor() as cursor:
        cursor.execute("""SELECT * FROM Movies M
        JOIN Bechdel B ON B.movieId = M.movieId 
        WHERE M.movieId=%s""", (movie_id, ))
        movie = cursor.fetchone()
        
        cursor.execute("SELECT * FROM MoviesGenres WHERE movieId=%s", (movie_id, ))
        genres = cursor.fetchall()
        movie['genres'] = [g['genre'] for g in genres]
        
        
        cursor.execute("""
            SELECT * FROM MoviesPeople MP
            JOIN People P on P.personId = MP.personId
            WHERE MP.movieId=%s
        """, (movie_id, ))
        people = cursor.fetchall()
        movie['people'] = people
        
        
    db_conn.close() 
    
    return movie
