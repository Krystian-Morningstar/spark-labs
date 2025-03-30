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
    
    # Filtrar solo los títulos de juegos para consolas PlayStation y Xbox
    query = """
        SELECT title, console
        FROM games
        WHERE console LIKE 'PS%' OR console LIKE 'XBOX%'
        ORDER BY console ASC
    """
    df_filtered = spark.sql(query)
    
    # Mostrar algunos resultados
    df_filtered.show(20)
    
    # Guardar los resultados en formato JSON
    df_filtered.write.mode("overwrite").json("results/video_game_sales")

    # Cerrar sesión de Spark
    spark.stop()
