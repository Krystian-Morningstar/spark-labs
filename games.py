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
    
    # Filtrar solo la columna 'title' y ordenar por 'console' alfabéticamente
    query = """
        SELECT title, console
        FROM games
        ORDER BY console ASC
    """
    df_filtered = spark.sql(query)
    
    # Mostrar resultados
    df_filtered.show(20)
    
    df_filtered.write.mode("overwrite").json("results/video_game_sales")
    
    # Cerrar sesión de Spark
    spark.stop()
