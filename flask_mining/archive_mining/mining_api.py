from flask import Flask, abort, request
#from flask_basicauth import BasicAuth
#from flask_swagger_ui import get_swaggerui_blueprint
import pymysql
import json
import math
import requests
from collections import defaultdict

app = Flask(__name__)
app.config.from_file("flask_config.json", load=json.load)
#auth = BasicAuth(app)

MAX_PAGE_SIZE = 10

#swaggerui_blueprint = get_swaggerui_blueprint(
#    base_url='/docs',
#    api_url='/static/openapi.yaml',
#)
#app.register_blueprint(swaggerui_blueprint)

@app.route("/products/<PRODUCT>")
#@auth.required
def product(PRODUCT):
    def remove_null_fields(obj):
        return {k:v for k, v in obj.items() if v is not None}
    db_conn = pymysql.connect(host="localhost", user="root", 
                              password="Eatyourdinner0991", 
                              database="mining",
                              cursorclass=pymysql.cursors.DictCursor)
    with db_conn.cursor() as cursor:
        cursor.execute("""SELECT
            P.PRODUCT AS product
            ,P.PRODUCT_GROUP AS product_group
            ,P.VOLUME AS volume
            ,P.MEASUREMENT_UNIT AS units
            ,P.VALUE AS value
            ,P.YEAR AS year
            ,L.STATE as state
            FROM PRODUCTS P
            LEFT JOIN locations L
                ON P.PRODUCT = L.PRODUCT                  
            WHERE P.PRODUCT=%s""", (PRODUCT, )
                       )
        PRODUCT = cursor.fetchone()
        if not PRODUCT:
            abort(404)
        else:
            remove_null_fields(PRODUCT)
    db_conn.close()
    return PRODUCT

@app.route("/products")
#@auth.required
def products():
    # URL parameters
    page = int(request.args.get('page', 0))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)
    include_details = bool(int(request.args.get('include_details', 0)))

    db_conn = pymysql.connect(host="localhost", user="root", 
                              password="Eatyourdinner0991",
                              database="mining",
                              cursorclass=pymysql.cursors.DictCursor)
    # Get the products
    with db_conn.cursor() as cursor:
        cursor.execute("""SELECT
            P.PRODUCT AS product
            ,P.PRODUCT_GROUP AS product_group
            ,P.VOLUME AS volume
            ,P.MEASUREMENT_UNIT AS units
            ,P.VALUE AS value
            ,P.YEAR AS year
            ,L.STATE AS state           
            FROM PRODUCTS P   
            JOIN locations L ON P.PRODUCT = L.PRODUCT    
            ORDER BY P.PRODUCT
            LIMIT %s
            OFFSET %s
        """, (page_size, page * page_size))
        products = cursor.fetchall()
        products = [p['product'] for p in products]
    # Get the total products count
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS total FROM products")
        total = cursor.fetchone()
        last_page = math.ceil(total['total'] / page_size)
    db_conn.close()
    return {
        'products': products,
        'last_page': f'/products?page={last_page}&page_size={page_size}',
        'next_page': f'/products?page={page+1}&page_size={page_size}',
    }
        
        
    
#    if include_details:
#        # Get genres
#        with db_conn.cursor() as cursor:
#            placeholder = ','.join(['%s'] * len(movie_ids))
#            cursor.execute(f"SELECT * FROM MoviesGenres WHERE movieId IN ({placeholder})",
#                        movie_ids)
#            genres = cursor.fetchall()
#        genres_dict = defaultdict(list)
#        for obj in genres:
#            genres_dict[obj['movieId']].append(obj['genre'])
#        
#        # Get people
#        with db_conn.cursor() as cursor:
#            placeholder = ','.join(['%s'] * len(movie_ids))
#            cursor.execute(f"""
#                SELECT
#                    MP.movieId,
#                    P.personId,
#                    P.primaryName AS name,
#                    P.birthYear,
#                    P.deathYear,
#                    MP.category AS role
#                FROM MoviesPeople MP
#                JOIN People P on P.personId = MP.personId
#                WHERE movieId IN ({placeholder})
#            """, movie_ids)
#            people = cursor.fetchall()
#        people_dict = defaultdict(list)
#        for obj in people:
#            movieId = obj['movieId']
#            del obj['movieId']
#            people_dict[movieId].append(obj)
#
#        # Merge genres and people into movies
#        for movie in movies:
#            movieId = movie['movieId']
#            movie['genres'] = genres_dict[movieId]
#            movie['people'] = people_dict[movieId]
#
#    # Get the total movies count
#    with db_conn.cursor() as cursor:
#        cursor.execute("SELECT COUNT(*) AS total FROM Movies")
#        total = cursor.fetchone()
#        last_page = math.ceil(total['total'] / page_size)
#    db_conn.close()
#    return {
#        'movies': movies,
#        'next_page': f'/movies?page={page+1}&page_size={page_size}',
#        'last_page': f'/movies?page={last_page}&page_size={page_size}',
#    }
#