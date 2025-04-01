from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Crear sesión de Spark
    spark = SparkSession\
        .builder\
        .appName("video_game_sales")\
        .getOrCreate()
    
    # Cargar el dataset
    path_games = "dataset.csv"
    df_games = spark.read.csv(path_games, header=True, inferSchema=True)
    
    # Crear vista temporal
    df_games.createOrReplaceTempView("games")
    
    popular_games = [
        "The Last of Us Part II", "God of War", "Spider-Man", 
        "Uncharted 4: A Thief's End", "Gran Turismo 7",
        "Halo: The Master Chief Collection", "Forza Horizon 4", 
        "Gears 5", "Minecraft", "Sea of Thieves"
    ]
    
    popular_consoles = ['PS3', 'PS4', 'XOne', 'Series']
    
    # Filtrar juegos populares de PS y Xbox
    query_popular = f"""
        SELECT title, console
        FROM games
        WHERE title IN {tuple(popular_games)}
        ORDER BY console ASC
    """
    df_filtered = spark.sql(query_popular)
    
    # Mostrar algunos resultados
    df_filtered.show(20)
    
    # Guardar los resultados en formato JSON
    df_filtered.write.mode("overwrite").json("results/video_game_sales")
    
    # Filtrar juegos de consolas populares con publisher y developer
    query_best = f"""
        SELECT title, console, publisher, developer
        FROM games
        WHERE console IN {tuple(popular_consoles)}
    """
    df_best = spark.sql(query_best)
    
    # Guardar los resultados en formato JSON en la nueva carpeta
    df_best.write.mode("overwrite").json("results/video_game_best")
    
    # Cerrar sesión de Spark
    spark.stop()
