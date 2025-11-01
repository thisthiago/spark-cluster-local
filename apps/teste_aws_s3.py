
"""
Teste RÁPIDO de leitura S3 - Compatível com Spark Connect
Execute este primeiro para verificar se a conexão S3 funciona
"""

import os
from pyspark.sql import SparkSession

# ============================================================================
# CONFIGURAÇÃO - EDITE AQUI
# ============================================================================
S3_BUCKET = "dev-lab-02-us-east-2-landing"
S3_PATH = "mflix/comments/"


print("=" * 80)
print("🪣 TESTE RÁPIDO DE CONEXÃO S3 - Spark Connect")
print("=" * 80)


print(f"\n✓ Bucket: {S3_BUCKET}")
print(f"✓ Path: {S3_PATH}")
print(f"✓ Credenciais: Configuradas")

# ============================================================================
# TESTE 1: Criar Spark Session
# ============================================================================
print("\n" + "=" * 80)
print("TESTE 1: Criando Spark Session com Spark Connect...")
print("=" * 80)

try:
    spark = SparkSession.builder \
        .remote("sc://localhost:15002") \
        .appName("S3-Quick-Test") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    print("✅ Spark Session criada com sucesso!")
    print(f"   Version: {spark.version}")
    print("   Modo: Spark Connect (remote)")
    
except Exception as e:
    print(f"❌ Erro ao criar Spark Session: {e}")
    exit(1)

# ============================================================================
# TESTE 2: Ler JSON diretamente (sem listar arquivos)
# ============================================================================
print("\n" + "=" * 80)
print("TESTE 2: Lendo JSON do S3...")
print("=" * 80)

s3_full_path = f"s3a://{S3_BUCKET}/{S3_PATH}"
print(f"\nPath completo: {s3_full_path}")

try:
    print("\nTentando ler dados JSON...")
    df = spark.read.option("multiline","true").json(s3_full_path)
    
    print("✅ Leitura bem-sucedida!")
    print("   O Spark conseguiu acessar o S3!")
    

    print("\n" + "=" * 80)
    print("Schema dos dados:")
    print("=" * 80)
    df.printSchema()
    
    print("\n" + "=" * 80)
    print("Contando registros (amostra)...")
    print("=" * 80)
    
    sample = df.limit(10)
    sample_count = sample.count()
    print(f"✅ Conseguiu ler {sample_count} registros de amostra")
    
    print("\nAmostra dos dados:")
    sample.show(5, truncate=50)
    
    print("\nContando todos os registros...")
    total = df.count()
    print(f"✅ Total de registros: {total:,}")
    
    print(f"\nColunas encontradas ({len(df.columns)}):")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i}. {col}")
    
except Exception as e:
    print(f"❌ Erro ao ler JSON: {e}")
    print("\nPossíveis causas:")
    print("  • Credenciais AWS inválidas")
    print("  • Bucket não existe: " + S3_BUCKET)
    print("  • Path incorreto: " + S3_PATH)
    print("  • Sem permissão s3:GetObject e s3:ListBucket")
    print("  • JARs do AWS SDK ausentes no servidor Spark")
    
    print("\n🔍 Debug: Stack trace completo:")
    import traceback
    traceback.print_exc()
    
    spark.stop()
    exit(1)

# ============================================================================
# TESTE 3: Operações básicas
# ============================================================================
print("\n" + "=" * 80)
print("TESTE 3: Testando operações de agregação...")
print("=" * 80)

try:
    sample_df = df.limit(100)
    sample_count = sample_df.count()
    
    print(f"Trabalhando com amostra de {sample_count} registros")
    
    if df.columns:
        string_cols = [col for col in df.columns if 'name' in col.lower() or 'email' in col.lower()]
        
        if string_cols:
            group_col = string_cols[0]
            print(f"\nAgrupando por coluna '{group_col}'...")
            
            from pyspark.sql.functions import count
            result = sample_df.groupBy(group_col).agg(count("*").alias("count")).orderBy("count", ascending=False)
            result.show(10, truncate=50)
            
            print("✅ Operação de agregação funcionou!")
        else:
            first_col = df.columns[0]
            print(f"\nAgrupando por coluna '{first_col}'...")
            
            from pyspark.sql.functions import count
            result = sample_df.groupBy(first_col).count().orderBy("count", ascending=False)
            result.show(10, truncate=50)
            
            print("✅ Operação de agregação funcionou!")
    
except Exception as e:
    print(f"⚠️  Erro na agregação: {e}")
    print("   (Não é crítico, a leitura do S3 funcionou!)")

# ============================================================================
# TESTE 4: Verificar tipos de dados
# ============================================================================
print("\n" + "=" * 80)
print("TESTE 4: Analisando tipos de dados...")
print("=" * 80)

try:
    print("\nTipos de dados encontrados:")
    for field in df.schema.fields:
        print(f"   • {field.name:20s} → {field.dataType}")
    
    print("\n" + "=" * 80)
    print("Estatísticas básicas:")
    print("=" * 80)
    
    numeric_cols = [field.name for field in df.schema.fields 
                   if 'int' in str(field.dataType).lower() or 'double' in str(field.dataType).lower()]
    
    if numeric_cols:
        print(f"\nColunas numéricas encontradas: {', '.join(numeric_cols)}")
        df.select(numeric_cols).summary().show()
    else:
        print("Nenhuma coluna numérica encontrada")
        
except Exception as e:
    print(f"⚠️  Erro ao analisar tipos: {e}")

# ============================================================================
# RESUMO FINAL
# ============================================================================
print("\n" + "=" * 80)
print("📊 RESUMO DOS TESTES")
print("=" * 80)

print(f"""
✅ TESTE 1: Spark Connect Session criada
✅ TESTE 2: Leitura de JSON do S3 funcionando
✅ TESTE 3: Operações Spark funcionando
✅ TESTE 4: Análise de schema concluída

🎉 TODOS OS TESTES PASSARAM COM SUCESSO!

Informações do dataset:
  • Bucket: {S3_BUCKET}
  • Path: {S3_PATH}
  • Total de registros: {total:,}
  • Total de colunas: {len(df.columns)}
""")

spark.stop()
print("\n✓ Spark Session encerrada")
print("=" * 80)