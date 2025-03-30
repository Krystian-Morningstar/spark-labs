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
    
    # Filtrar juegos populares de PS y Xbox
    query = f"""
        SELECT title, console
        FROM games
        WHERE title IN {tuple(popular_games)}
        ORDER BY console ASC
    """
    df_filtered = spark.sql(query)
    
    # Mostrar algunos resultados
    df_filtered.show(20)
    
    # Guardar los resultados en formato JSON
    df_filtered.write.mode("overwrite").json("results/video_game_sales")

    # Cerrar sesión de Spark
    spark.stop()
