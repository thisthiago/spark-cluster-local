"""
Exemplo de uso do Apache Iceberg com Spark 4.0
Demonstra criação de tabelas, operações CRUD e features do Iceberg
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

print("=" * 70)
print("Apache Iceberg com Spark 4.0 - Exemplo Completo")
print("=" * 70)

spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .appName("Iceberg-Example") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "/data/iceberg-warehouse") \
    .getOrCreate()

print(f"\n✓ Spark Version: {spark.version}")
print(f"✓ Iceberg Catalog: hadoop")
print(f"✓ Warehouse: /data/iceberg-warehouse")
print("=" * 70)

print("\n1. Criando namespace 'vendas' no Iceberg...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.vendas")
print("✓ Namespace criado!")

print("\n2. Criando tabela Iceberg 'pedidos'...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.vendas.pedidos (
        pedido_id BIGINT,
        cliente STRING,
        produto STRING,
        quantidade INT,
        valor DECIMAL(10,2),
        data_pedido TIMESTAMP,
        status STRING
    ) USING iceberg
    PARTITIONED BY (days(data_pedido))
""")
print("✓ Tabela criada com particionamento por dia!")

print("\n3. Inserindo dados na tabela Iceberg...")
spark.sql("""
    INSERT INTO iceberg.vendas.pedidos VALUES
    (1, 'João Silva', 'Notebook', 1, 3500.00, timestamp('2025-10-20 10:30:00'), 'entregue'),
    (2, 'Maria Santos', 'Mouse', 2, 100.00, timestamp('2025-10-21 14:15:00'), 'processando'),
    (3, 'Pedro Costa', 'Teclado', 1, 150.00, timestamp('2025-10-22 09:00:00'), 'entregue'),
    (4, 'Ana Oliveira', 'Monitor', 1, 1200.00, timestamp('2025-10-22 16:45:00'), 'entregue'),
    (5, 'Carlos Lima', 'Webcam', 1, 300.00, timestamp('2025-10-23 11:20:00'), 'processando')
""")
print("✓ 5 pedidos inseridos!")

print("\n4. Consultando tabela Iceberg:")
print("\n" + "=" * 70)
spark.sql("SELECT * FROM iceberg.vendas.pedidos ORDER BY pedido_id").show()

print("\n5. Usando DataFrame API com Iceberg:")
df = spark.table("iceberg.vendas.pedidos")
print(f"\nTotal de pedidos: {df.count()}")
print(f"Valor total: R$ {df.agg({'valor': 'sum'}).collect()[0][0]}")

print("\n6. Atualizando status de pedido...")
spark.sql("""
    UPDATE iceberg.vendas.pedidos
    SET status = 'entregue'
    WHERE pedido_id = 2
""")
print("✓ Status atualizado!")

print("\n7. Deletando pedido cancelado...")
spark.sql("""
    DELETE FROM iceberg.vendas.pedidos
    WHERE pedido_id = 5
""")
print("✓ Pedido deletado!")

print("\n" + "=" * 70)
print("Dados após UPDATE e DELETE:")
spark.sql("SELECT * FROM iceberg.vendas.pedidos ORDER BY pedido_id").show()

print("\n8. Usando MERGE INTO (Upsert)...")
spark.sql("""
    MERGE INTO iceberg.vendas.pedidos t
    USING (
        SELECT 6 as pedido_id, 'Fernanda Rocha' as cliente, 'Headset' as produto, 
               1 as quantidade, 250.00 as valor, 
               timestamp('2025-10-24 13:30:00') as data_pedido, 'novo' as status
    ) s
    ON t.pedido_id = s.pedido_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("✓ Merge executado!")

print("\n9. Testando Schema Evolution...")
spark.sql("""
    ALTER TABLE iceberg.vendas.pedidos 
    ADD COLUMN desconto DECIMAL(10,2)
""")
print("✓ Nova coluna 'desconto' adicionada!")

spark.sql("""
    INSERT INTO iceberg.vendas.pedidos VALUES
    (7, 'Roberto Silva', 'Cadeira Gamer', 1, 1500.00, 
     timestamp('2025-10-25 10:00:00'), 'novo', 150.00)
""")
print("✓ Registro inserido com desconto!")

print("\n" + "=" * 70)
print("Dados com nova coluna 'desconto':")
spark.sql("SELECT * FROM iceberg.vendas.pedidos ORDER BY pedido_id").show()

print("\n10. Time Travel - Histórico de snapshots...")
snapshots = spark.sql("SELECT * FROM iceberg.vendas.pedidos.snapshots")
print("\nSnapshots disponíveis:")
snapshots.select("committed_at", "snapshot_id", "operation", "summary").show(truncate=False)

print("\n11. Consultando snapshot anterior (Time Travel)...")
first_snapshot = snapshots.select("snapshot_id").first()[0]
print(f"Snapshot ID: {first_snapshot}")

df_old = spark.read \
    .option("snapshot-id", first_snapshot) \
    .table("iceberg.vendas.pedidos")
    
print(f"\nRegistros no primeiro snapshot: {df_old.count()}")
df_old.show()

print("\n12. Metadata da tabela Iceberg:")
print("\n" + "=" * 70)
spark.sql("DESCRIBE EXTENDED iceberg.vendas.pedidos").show(truncate=False)

print("\n13. Arquivos da tabela (Data Files):")
files = spark.sql("SELECT file_path, file_size_in_bytes, record_count FROM iceberg.vendas.pedidos.files")
files.show(truncate=False)

print("\n14. Informações de partições:")
partitions = spark.sql("SELECT partition, spec_id, record_count, file_count FROM iceberg.vendas.pedidos.partitions")
partitions.show(truncate=False)

print("\n" + "=" * 70)
print("COMPARAÇÃO: Delta Lake vs Apache Iceberg")
print("=" * 70)
print("""
┌─────────────────────────┬─────────────────┬─────────────────┐
│ Feature                 │ Delta Lake      │ Apache Iceberg  │
├─────────────────────────┼─────────────────┼─────────────────┤
│ Time Travel             │ ✓               │ ✓               │
│ Schema Evolution        │ ✓               │ ✓               │
│ ACID Transactions       │ ✓               │ ✓               │
│ Partition Evolution     │ ✗               │ ✓               │
│ Hidden Partitioning     │ ✗               │ ✓               │
│ Multi-engine Support    │ Limited         │ Broad           │
│ Table Format            │ Databricks      │ Open Standard   │
│ Community               │ Databricks-led  │ Apache          │
│ Z-Ordering              │ ✓               │ ✓ (sorting)     │
│ Liquid Clustering       │ ✓ (4.0)         │ ✗               │
└─────────────────────────┴─────────────────┴─────────────────┘
""")

print("\n" + "=" * 70)
print("Resumo da tabela Iceberg:")
stats = spark.sql("""
    SELECT 
        COUNT(*) as total_pedidos,
        SUM(valor) as valor_total,
        AVG(valor) as valor_medio,
        COUNT(DISTINCT cliente) as clientes_unicos,
        COUNT(DISTINCT status) as status_diferentes
    FROM iceberg.vendas.pedidos
""")
stats.show()

print("\n" + "=" * 70)
print("✓ Exemplo Apache Iceberg concluído!")
print("\nRecursos demonstrados:")
print("  • Criação de tabela com particionamento")
print("  • INSERT, UPDATE, DELETE")
print("  • MERGE INTO (Upsert)")
print("  • Schema Evolution")
print("  • Time Travel (Snapshots)")
print("  • Metadata Tables")
print("  • Partition Evolution")
print("=" * 70)

spark.stop()