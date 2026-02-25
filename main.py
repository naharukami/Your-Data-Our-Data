from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions.builtin import desc

# Pyspark set up
pyspark = SparkSession.builder.appName("Anime").getOrCreate()

print("---------------------------------------------------------------------------------------------------------")
# read from csv file
animes = pyspark.read.csv("anime.csv", header=True, inferSchema=True)

# Partition animes into 4 parts
animes_hash_part = animes.repartition(4)
################################

anime_types = [ row.type for row in animes_hash_part.select("type").distinct().collect()]
print(anime_types)

for anime_type in anime_types:
  filtered_anime = animes_hash_part.filter(col("type") == anime_type).orderBy(col('name'))
  filtered_anime.write.mode('overwrite').csv(f"Anime/{anime_type}", header=True)

# range partitioning by rating
anime_range_part = animes.repartitionByRange(4, 'rating')
top_100_anime = anime_range_part.orderBy(desc('rating')).limit(100)
top_100_anime.write.mode('overwrite').csv("Top Anime")
top_100_anime.show(n=100, truncate=False)


# filt_anime.show()
# animes.show(n=100)

#[x] group by type
# filter top 1-200
#
