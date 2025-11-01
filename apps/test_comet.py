"""
Teste Apache DataFusion Comet com Spark 4.0
Demonstra o acelerador nativo e compara performance
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, round as spark_round
import time

print("=" * 80)
print("Apache DataFusion Comet - Teste de Aceleração com Spark 4.0")
print("=" * 80)

# ============================================================================
# TESTE 1: Sem Comet (baseline)
# ============================================================================
print("\n" + "=" * 80)
print("TESTE 1: Executando SEM Comet (Baseline)")
print("=" * 80)

spark_baseline = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .appName("Baseline-No-Comet") \
    .getOrCreate()

print(f"✓ Spark Version: {spark_baseline.version}")
print("✓ Comet: DESABILITADO")

print("\n1. Criando dataset de teste (1 milhão de registros)...")
df = spark_baseline.range(0, 1_000_000).toDF("id")
df = df.withColumn("categoria", (col("id") % 100).cast("string"))
df = df.withColumn("valor", (col("id") * 1.5).cast("double"))
df = df.withColumn("quantidade", (col("id") % 50).cast("int"))

print("2. Salvando dados em Parquet...")
parquet_path = "/data/test_comet_data"
df.write.mode("overwrite").parquet(parquet_path)
print(f"✓ Dados salvos em: {parquet_path}")

print("\n3. Executando query complexa (GROUP BY + AGGREGATIONS)...")
start_time = time.time()

result_baseline = spark_baseline.read.parquet(parquet_path) \
    .groupBy("categoria") \
    .agg(
        count("id").alias("total_registros"),
        spark_sum("valor").alias("soma_valores"),
        avg("valor").alias("media_valores"),
        spark_sum("quantidade").alias("soma_quantidade")
    ) \
    .orderBy("categoria")

count_baseline = result_baseline.count()
baseline_time = time.time() - start_time

print(f"\n✓ Query concluída!")
print(f"✓ Registros processados: {count_baseline}")
print(f"⏱️  Tempo SEM Comet: {baseline_time:.3f} segundos")

print("\n4. Query Plan (SEM Comet):")
print("-" * 80)
result_baseline.explain(mode="simple")

print("\n5. Amostra dos resultados:")
result_baseline.show(10, truncate=False)

spark_baseline.stop()

# ============================================================================
# TESTE 2: Com Comet (acelerado)
# ============================================================================
print("\n" + "=" * 80)
print("TESTE 2: Executando COM Comet (Acelerado)")
print("=" * 80)

spark_comet = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .appName("Accelerated-With-Comet") \
    .config("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions") \
    .config("spark.comet.enabled", "true") \
    .config("spark.comet.exec.enabled", "true") \
    .config("spark.comet.exec.all.enabled", "true") \
    .config("spark.comet.exec.shuffle.enabled", "true") \
    .config("spark.shuffle.manager", "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager") \
    .getOrCreate()

print(f"✓ Spark Version: {spark_comet.version}")
print("✓ Comet: HABILITADO")

print("\n1. Executando mesma query COM Comet...")
start_time = time.time()

result_comet = spark_comet.read.parquet(parquet_path) \
    .groupBy("categoria") \
    .agg(
        count("id").alias("total_registros"),
        spark_sum("valor").alias("soma_valores"),
        avg("valor").alias("media_valores"),
        spark_sum("quantidade").alias("soma_quantidade")
    ) \
    .orderBy("categoria")

count_comet = result_comet.count()
comet_time = time.time() - start_time

print(f"\n✓ Query concluída!")
print(f"✓ Registros processados: {count_comet}")
print(f"⏱️  Tempo COM Comet: {comet_time:.3f} segundos")

print("\n2. Query Plan (COM Comet - verificar CometScan/CometExec):")
print("-" * 80)
result_comet.explain(mode="extended")

print("\n3. Amostra dos resultados:")
result_comet.show(10, truncate=False)

spark_comet.stop()

# ============================================================================
# COMPARAÇÃO DE PERFORMANCE
# ============================================================================
print("\n" + "=" * 80)
print("COMPARAÇÃO DE PERFORMANCE")
print("=" * 80)

speedup = baseline_time / comet_time if comet_time > 0 else 0
improvement = ((baseline_time - comet_time) / baseline_time * 100) if baseline_time > 0 else 0

print(f"""
┌─────────────────────────────────────────────────────────────┐
│                    RESULTADOS DO TESTE                      │
├─────────────────────────────────────────────────────────────┤
│ Dataset:                1,000,000 registros                 │
│ Query:                  GROUP BY + 4 aggregations           │
│                                                             │
│ Tempo SEM Comet:        {baseline_time:>6.3f} segundos      |
│ Tempo COM Comet:        {comet_time:>6.3f} segundos         │
│                                                             │
│ Speedup:                {speedup:>6.2f}x                    │
│ Melhoria:               {improvement:>6.2f}%                |
└─────────────────────────────────────────────────────────────┘
""")

if speedup > 1.1:
    print("✅ Comet está acelerando as queries!")
    print(f"   Performance melhorou em {improvement:.1f}%")
elif speedup < 0.9:
    print("⚠️  Comet está mais lento que o baseline")
    print("   Possíveis causas:")
    print("   • Dataset muito pequeno (overhead do Comet)")
    print("   • Comet spark3.5 pode ter limitações com Spark 4.0")
    print("   • Operações não suportadas pelo Comet")
else:
    print("➡️  Performance similar (diferença < 10%)")
    print("   Normal para datasets pequenos")

# ============================================================================
# VERIFICAÇÃO DE COMPATIBILIDADE
# ============================================================================
print("\n" + "=" * 80)
print("VERIFICAÇÃO DE COMPATIBILIDADE")
print("=" * 80)

print("""
⚠️  IMPORTANTE: Comet spark3.5_2.13-0.9.0 foi testado com Spark 3.5

Para Spark 4.0, você pode encontrar:
  • Alguns recursos não suportados
  • Fallback automático para engine Spark padrão
  • Performance pode variar dependendo da query

Verificações para confirmar que Comet está funcionando:
  ✓ No EXPLAIN, procure por: CometScan, CometExec
  ✓ Logs devem mostrar: "Comet extension initialized"
  ✓ Queries com scan de Parquet devem mostrar melhoria

Para melhor compatibilidade com Spark 4.0:
  • Aguardar release oficial do Comet para Spark 4.0
  • Ou usar Spark 3.5 que é totalmente suportado
""")

print("=" * 80)
print("✓ Teste concluído!")
print("\nRecursos testados:")
print("  • Leitura de Parquet com Comet")
print("  • Aggregações (COUNT, SUM, AVG)")
print("  • GROUP BY")
print("  • Comparação de performance")
print("  • Verificação de query plans")
print("=" * 80)