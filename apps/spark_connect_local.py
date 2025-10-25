"""
Script para rodar LOCALMENTE (no seu computador) usando Spark Connect
Conecta ao cluster Spark rodando no Docker

Instalação necessária:
pip install pyspark==4.0.1 delta-spark==4.0.1
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max as spark_max

print("=" * 70)
print("Conectando ao Spark Connect Server...")
print("URL: sc://localhost:15002")
print("=" * 70)


spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .appName("SparkConnect-Local-Client") \
    .getOrCreate()

print(f"\n Conectado com sucesso!")
print(f"Spark Version: {spark.version}")
print(f"Executando do seu computador local!")
print("=" * 70)


print("\nCriando dados de exemplo...")
data = [
    {"produto": "Notebook", "preco": 3500.00, "categoria": "Eletrônicos"},
    {"produto": "Mouse", "preco": 50.00, "categoria": "Periféricos"},
    {"produto": "Teclado", "preco": 150.00, "categoria": "Periféricos"},
    {"produto": "Monitor", "preco": 1200.00, "categoria": "Eletrônicos"},
    {"produto": "Cadeira", "preco": 800.00, "categoria": "Móveis"},
    {"produto": "Mesa", "preco": 1500.00, "categoria": "Móveis"},
]

df = spark.createDataFrame(data)

print("\n" + "=" * 70)
print("Dados criados:")
df.show()


print("\n" + "=" * 70)
print("Produtos com preço > 500:")
df_filtrado = df.filter(col("preco") > 500)
df_filtrado.show()

print("\n" + "=" * 70)
print("Estatísticas por categoria:")
df_stats = df.groupBy("categoria").agg(
    count("*").alias("total_produtos"),
    avg("preco").alias("preco_medio"),
    spark_max("preco").alias("preco_maximo")
).orderBy(col("preco_medio").desc())

df_stats.show()

df.createOrReplaceTempView("produtos")

print("\n" + "=" * 70)
print("Top 3 produtos mais caros:")
df_top = spark.sql("""
    SELECT produto, preco, categoria
    FROM produtos
    ORDER BY preco DESC
    LIMIT 3
""")
df_top.show()

print("\n" + "=" * 70)
print("Salvando em Delta Lake...")
delta_path = "/data/delta/produtos"

df.write.format("delta").mode("overwrite").save(delta_path)
print(f" Dados salvos em: {delta_path}")

print("\nLendo Delta Table:")
df_delta = spark.read.format("delta").load(delta_path)
df_delta.show()

print("\n" + "=" * 70)
print("Adicionando novos produtos...")
novos_produtos = [
    {"produto": "Webcam", "preco": 300.00, "categoria": "Periféricos"},
    {"produto": "Headset", "preco": 250.00, "categoria": "Periféricos"},
]

df_novos = spark.createDataFrame(novos_produtos)
df_novos.write.format("delta").mode("append").save(delta_path)

print(" Novos produtos adicionados!")
print("\nDados completos:")
df_completo = spark.read.format("delta").load(delta_path)
df_completo.orderBy("categoria", "preco").show()

print("\n" + "=" * 70)
print("Ranking de produtos por categoria:")

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank

windowSpec = Window.partitionBy("categoria").orderBy(col("preco").desc())

df_ranking = df_completo.withColumn("ranking", rank().over(windowSpec))
df_ranking.orderBy("categoria", "ranking").show()

print("\n" + "=" * 70)
print("Informações sobre Spark Connect:")
print(f"  • Cliente Python local: ~1.5 MB")
print(f"  • Conexão: gRPC (port 15002)")
print(f"  • Processamento: No cluster Docker")
print(f"  • Isolamento: Total entre cliente e servidor")

total_produtos = df_completo.count()
valor_total = df_completo.agg({"preco": "sum"}).collect()[0][0]
preco_medio = df_completo.agg({"preco": "avg"}).collect()[0][0]

print("\n" + "=" * 70)
print("Resumo Final:")
print(f"  Total de produtos: {total_produtos}")
print(f"  Valor total: R$ {valor_total:,.2f}")
print(f"  Preço médio: R$ {preco_medio:,.2f}")
print("=" * 70)

print("\n Fim :D !")
spark.stop()