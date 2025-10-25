"""
Comparação prática entre Delta Lake e Apache Iceberg
Demonstra as mesmas operações em ambos os formatos
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import time

print("=" * 70)
print("Delta Lake vs Apache Iceberg - Comparação Prática")
print("=" * 70)

# Criar SparkSession com ambos configurados
spark = SparkSession.builder \
    .appName("Delta-vs-Iceberg-Comparison") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "/data/iceberg-warehouse") \
    .getOrCreate()

# Dados de exemplo
data = [
    (1, "Produto A", 100, 10.50, "2025-10-20"),
    (2, "Produto B", 200, 20.00, "2025-10-21"),
    (3, "Produto C", 150, 15.75, "2025-10-22"),
    (4, "Produto D", 300, 30.00, "2025-10-23"),
    (5, "Produto E", 250, 25.50, "2025-10-24"),
]

columns = ["id", "nome", "quantidade", "preco", "data"]
df = spark.createDataFrame(data, columns)

print("\n📊 Dados de teste:")
df.show()

# ========== DELTA LAKE ==========
print("\n" + "=" * 70)
print("🔷 DELTA LAKE")
print("=" * 70)

# 1. Criar tabela Delta
print("\n1. Criando tabela Delta...")
start = time.time()
delta_path = "/data/delta/produtos"
df.write.format("delta").mode("overwrite").save(delta_path)
delta_create_time = time.time() - start
print(f"✓ Tabela Delta criada em {delta_create_time:.3f}s")

# 2. Ler Delta
print("\n2. Lendo tabela Delta...")
start = time.time()
df_delta = spark.read.format("delta").load(delta_path)
delta_read_time = time.time() - start
print(f"✓ Leitura concluída em {delta_read_time:.3f}s")
print(f"   Registros: {df_delta.count()}")

# 3. Update Delta
print("\n3. Update em Delta...")
start = time.time()
from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, delta_path)
deltaTable.update(
    condition = "id = 1",
    set = {"preco": "12.00"}
)
delta_update_time = time.time() - start
print(f"✓ Update concluído em {delta_update_time:.3f}s")

# 4. Delete Delta
print("\n4. Delete em Delta...")
start = time.time()
deltaTable.delete("id = 5")
delta_delete_time = time.time() - start
print(f"✓ Delete concluído em {delta_delete_time:.3f}s")

# 5. Merge Delta
print("\n5. Merge (Upsert) em Delta...")
start = time.time()
updates = spark.createDataFrame([
    (6, "Produto F", 400, 40.00, "2025-10-25"),
    (1, "Produto A+", 100, 12.00, "2025-10-20")
], columns)

deltaTable.alias("target").merge(
    updates.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
delta_merge_time = time.time() - start
print(f"✓ Merge concluído em {delta_merge_time:.3f}s")

# 6. Time Travel Delta
print("\n6. Time Travel em Delta...")
history = deltaTable.history()
print(f"   Versões disponíveis: {history.count()}")
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
print(f"   Registros na versão 0: {df_v0.count()}")

# 7. Schema Evolution Delta
print("\n7. Schema Evolution em Delta...")
start = time.time()
df_new_schema = spark.createDataFrame([
    (7, "Produto G", 500, 50.00, "2025-10-26", "Nova Categoria")
], columns + ["categoria"])
df_new_schema.write.format("delta").mode("append") \
    .option("mergeSchema", "true").save(delta_path)
delta_schema_time = time.time() - start
print(f"✓ Schema Evolution em {delta_schema_time:.3f}s")

# ========== APACHE ICEBERG ==========
print("\n" + "=" * 70)
print("🔶 APACHE ICEBERG")
print("=" * 70)

# 1. Criar tabela Iceberg
print("\n1. Criando tabela Iceberg...")
start = time.time()
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.comparacao")
df.write.format("iceberg") \
    .mode("overwrite") \
    .saveAsTable("iceberg.comparacao.produtos")
iceberg_create_time = time.time() - start
print(f"✓ Tabela Iceberg criada em {iceberg_create_time:.3f}s")

# 2. Ler Iceberg
print("\n2. Lendo tabela Iceberg...")
start = time.time()
df_iceberg = spark.table("iceberg.comparacao.produtos")
iceberg_read_time = time.time() - start
print(f"✓ Leitura concluída em {iceberg_read_time:.3f}s")
print(f"   Registros: {df_iceberg.count()}")

# 3. Update Iceberg
print("\n3. Update em Iceberg...")
start = time.time()
spark.sql("""
    UPDATE iceberg.comparacao.produtos
    SET preco = 12.00
    WHERE id = 1
""")
iceberg_update_time = time.time() - start
print(f"✓ Update concluído em {iceberg_update_time:.3f}s")

# 4. Delete Iceberg
print("\n4. Delete em Iceberg...")
start = time.time()
spark.sql("DELETE FROM iceberg.comparacao.produtos WHERE id = 5")
iceberg_delete_time = time.time() - start
print(f"✓ Delete concluído em {iceberg_delete_time:.3f}s")

# 5. Merge Iceberg
print("\n5. Merge (Upsert) em Iceberg...")
start = time.time()
updates.createOrReplaceTempView("updates")
spark.sql("""
    MERGE INTO iceberg.comparacao.produtos t
    USING updates s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
iceberg_merge_time = time.time() - start
print(f"✓ Merge concluído em {iceberg_merge_time:.3f}s")

# 6. Time Travel Iceberg
print("\n6. Time Travel em Iceberg...")
snapshots = spark.sql("SELECT * FROM iceberg.comparacao.produtos.snapshots")
print(f"   Snapshots disponíveis: {snapshots.count()}")
first_snapshot = snapshots.select("snapshot_id").first()[0]
df_snap = spark.read.option("snapshot-id", first_snapshot) \
    .table("iceberg.comparacao.produtos")
print(f"   Registros no primeiro snapshot: {df_snap.count()}")

# 7. Schema Evolution Iceberg
print("\n7. Schema Evolution em Iceberg...")
start = time.time()
spark.sql("""
    ALTER TABLE iceberg.comparacao.produtos 
    ADD COLUMN categoria STRING
""")
spark.sql("""
    INSERT INTO iceberg.comparacao.produtos
    VALUES (7, 'Produto G', 500, 50.00, '2025-10-26', 'Nova Categoria')
""")
iceberg_schema_time = time.time() - start
print(f"✓ Schema Evolution em {iceberg_schema_time:.3f}s")

# ========== COMPARAÇÃO DE PERFORMANCE ==========
print("\n" + "=" * 70)
print("📊 COMPARAÇÃO DE PERFORMANCE")
print("=" * 70)

print(f"""
┌──────────────────────┬──────────────┬──────────────┬──────────────┐
│ Operação             │ Delta Lake   │ Iceberg      │ Vencedor     │
├──────────────────────┼──────────────┼──────────────┼──────────────┤
│ CREATE TABLE         │ {delta_create_time:>8.3f}s    │ {iceberg_create_time:>8.3f}s    │ {'Delta' if delta_create_time < iceberg_create_time else 'Iceberg':>12s} │
│ READ                 │ {delta_read_time:>8.3f}s    │ {iceberg_read_time:>8.3f}s    │ {'Delta' if delta_read_time < iceberg_read_time else 'Iceberg':>12s} │
│ UPDATE               │ {delta_update_time:>8.3f}s    │ {iceberg_update_time:>8.3f}s    │ {'Delta' if delta_update_time < iceberg_update_time else 'Iceberg':>12s} │
│ DELETE               │ {delta_delete_time:>8.3f}s    │ {iceberg_delete_time:>8.3f}s    │ {'Delta' if delta_delete_time < iceberg_delete_time else 'Iceberg':>12s} │
│ MERGE (Upsert)       │ {delta_merge_time:>8.3f}s    │ {iceberg_merge_time:>8.3f}s    │ {'Delta' if delta_merge_time < iceberg_merge_time else 'Iceberg':>12s} │
│ SCHEMA EVOLUTION     │ {delta_schema_time:>8.3f}s    │ {iceberg_schema_time:>8.3f}s    │ {'Delta' if delta_schema_time < iceberg_schema_time else 'Iceberg':>12s} │
└──────────────────────┴──────────────┴──────────────┴──────────────┘
""")

# ========== FEATURES EXCLUSIVAS ==========
print("\n" + "=" * 70)
print("⚡ FEATURES EXCLUSIVAS")
print("=" * 70)

print("\n🔷 Delta Lake 4.0:")
print("  • Liquid Clustering (otimização automática)")
print("  • Delta Connect (integração com Spark Connect)")
print("  • Melhor integração com Databricks")
print("  • Z-Ordering nativo")
print("  • Change Data Feed (CDC)")
print("  • Deletion Vectors (performance em deletes)")

print("\n🔶 Apache Iceberg:")
print("  • Partition Evolution (mudar particionamento sem reescrever)")
print("  • Hidden Partitioning (usuário não vê partições)")
print("  • Multi-engine support (Spark, Flink, Trino, Presto)")
print("  • Table Format aberto (Apache Foundation)")
print("  • Metadata Tables robustas")
print("  • Sort Order customizável")

# ========== QUANDO USAR CADA UM ==========
print("\n" + "=" * 70)
print("🎯 QUANDO USAR?")
print("=" * 70)

print("\n✅ Use DELTA LAKE quando:")
print("  • Já usa Databricks")
print("  • Precisa de Liquid Clustering")
print("  • Quer integração nativa com Delta Live Tables")
print("  • Precisa de Change Data Feed")
print("  • Performance é prioridade máxima")
print("  • Ecosystem Databricks é importante")

print("\n✅ Use APACHE ICEBERG quando:")
print("  • Precisa de multi-engine support (Trino, Flink, etc)")
print("  • Quer formato open source neutro")
print("  • Precisa de Partition Evolution")
print("  • Trabalha em ambiente multi-cloud")
print("  • Prefere padrões Apache")
print("  • Precisa de flexibilidade máxima")

# ========== ANÁLISE DE METADADOS ==========
print("\n" + "=" * 70)
print("📁 ANÁLISE DE METADADOS")
print("=" * 70)

print("\n🔷 Delta Lake - Tamanho de arquivos:")
import os
delta_size = 0
for root, dirs, files in os.walk("/data/delta/produtos"):
    for file in files:
        delta_size += os.path.getsize(os.path.join(root, file))
print(f"   Total: {delta_size / 1024:.2f} KB")

print("\n🔶 Iceberg - Metadata Tables:")
print("\n   Files:")
spark.sql("SELECT COUNT(*) as total_files FROM iceberg.comparacao.produtos.files").show()
print("\n   Snapshots:")
spark.sql("SELECT COUNT(*) as total_snapshots FROM iceberg.comparacao.produtos.snapshots").show()
print("\n   Manifests:")
spark.sql("SELECT COUNT(*) as total_manifests FROM iceberg.comparacao.produtos.manifests").show()

# ========== CONSULTAS FINAIS ==========
print("\n" + "=" * 70)
print("📋 DADOS FINAIS (Delta Lake):")
print("=" * 70)
df_delta_final = spark.read.format("delta").load(delta_path)
df_delta_final.orderBy("id").show()

print("\n" + "=" * 70)
print("📋 DADOS FINAIS (Iceberg):")
print("=" * 70)
df_iceberg_final = spark.table("iceberg.comparacao.produtos")
df_iceberg_final.orderBy("id").show()

# ========== VERIFICAÇÃO DE CONSISTÊNCIA ==========
print("\n" + "=" * 70)
print("🔍 VERIFICAÇÃO DE CONSISTÊNCIA")
print("=" * 70)

delta_count = df_delta_final.count()
iceberg_count = df_iceberg_final.count()

print(f"\n   Delta Lake registros: {delta_count}")
print(f"   Iceberg registros: {iceberg_count}")

if delta_count == iceberg_count:
    print("\n   ✅ Ambos têm a mesma quantidade de registros!")
else:
    print("\n   ⚠️  Diferença na quantidade de registros")

# ========== RECOMENDAÇÕES ==========
print("\n" + "=" * 70)
print("💡 RECOMENDAÇÕES FINAIS")
print("=" * 70)
print("""
Para um cluster Spark moderno, considere:

1. 🎯 CENÁRIO ÚNICO (Uma engine):
   → Use Delta Lake se já está no ecossistema Databricks
   → Use Iceberg se prefere Apache open source

2. 🌐 CENÁRIO MULTI-ENGINE (Spark + Trino + Flink):
   → Use Iceberg para máxima compatibilidade

3. 🚀 PERFORMANCE CRÍTICA:
   → Delta Lake com Liquid Clustering (Spark 4.0)
   → Iceberg com Sort Order bem configurado

4. 🔄 EVOLUTIVO (Schema/Partition changes frequentes):
   → Iceberg para Partition Evolution
   → Delta Lake para Schema Evolution simples

5. 💰 CUSTO (Storage):
   → Ambos são eficientes com compaction
   → Iceberg pode ser ligeiramente mais eficiente em metadata

🏆 VENCEDOR GERAL: Depende do seu caso de uso!
   • Delta Lake: Melhor para ecosistema Databricks
   • Iceberg: Melhor para multi-engine e neutralidade
""")

print("\n" + "=" * 70)
print("✅ Comparação concluída!")
print("=" * 70)

spark.stop()