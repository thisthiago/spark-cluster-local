# Cluster Apache Spark 4.0 com Hadoop e Delta Lake

Este setup fornece um cluster Spark 4.0 completo com Apache Hadoop, Delta Lake 4.0, History Server, Spark UI e Spark Connect habilitados.

## 🚀 Componentes

- **Apache Spark 4.0.1** (lançado em setembro de 2025)
- **Apache Hadoop 3.4.1**
- **Delta Lake 4.0.1**
- **Apache Iceberg 1.7.1**
- **Java 17** (padrão no Spark 4.0)
- **Spark Connect** (cliente-servidor)
- **Spark History Server**
- **Spark Master + 2 Workers**

## ⭐ Novidades do Spark 4.0

- **ANSI Mode por padrão**: Melhor conformidade SQL
- **Java 17**: Runtime moderno e melhor performance
- **VARIANT Data Type**: Para dados semi-estruturados
- **Spark Connect**: Arquitetura cliente-servidor leve (1.5 MB)
- **Delta Lake 4.0**: Com Delta Connect e Liquid Clustering
- **Apache Iceberg 1.7**: Suporte completo para table format aberto
- **Melhorias em Streaming**: TransformWithState aprimorado
- **Python 3.8+**: Suporte a pandas 2.x

## 📋 Estrutura do Projeto

```
.
├── Dockerfile
├── docker-compose.yml
├── apps/                      # Suas aplicações Spark
├── data/                      # Dados e Delta Tables
│   ├── delta/                 # Delta Lake tables
│   └── iceberg-warehouse/     # Iceberg tables
└── spark-events/              # Logs de eventos para History Server
```

## 🔧 Como Usar

### 1. Build e Iniciar o Cluster

```bash
docker build -t spark-local-cluster:latest .

docker-compose up -d

sleep 30

docker-compose logs -f spark-master
```

### 2. Verificar se Spark Connect está rodando

```bash
docker exec spark-master ps aux | grep connect

docker logs spark-master | grep -i connect

python test_connection.py
```

### 3. Verificar Status

Acesse as UIs:

- **Spark Master UI**: http://localhost:8080
- **Spark Worker 1 UI**: http://localhost:8081
- **Spark Worker 2 UI**: http://localhost:8082
- **History Server**: http://localhost:18080
- **Application UI**: http://localhost:4040 (quando uma app estiver rodando)
- **Spark Connect**: grpc://localhost:15002 (novo no Spark 4.0)

### 5. Submeter via spark-submit (Modo Tradicional)

#### Aplicação Python:

```bash
docker cp seu_app.py spark-master:/apps/

docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages io.delta:delta-spark_2.13:4.0.1 \
  /apps/seu_app.py
```

#### Aplicação Scala/Java (JAR):

```bash
docker cp seu_app.jar spark-master:/apps/

docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --class com.example.MainClass \
  --packages io.delta:delta-spark_2.13:4.0.1 \
  /apps/seu_app.jar
```

### 6. Interagir com Spark Shell

### 4. Submeter Aplicação via Spark Connect (Recomendado no Spark 4.0)

O Spark Connect permite rodar aplicações remotamente com cliente leve (1.5 MB):

#### Dentro do container:

```bash
docker cp spark_app_example.py spark-master:/apps/
docker exec -it spark-master python /apps/spark_app_example.py
```

#### Do seu computador local (recomendado):

```bash
pip install pyspark==4.0.1 delta-spark==4.0.1

python spark_connect_local.py
```

**Vantagens do Spark Connect:**
- Cliente muito mais leve (1.5 MB vs 355 MB)
- Aplicação roda fora do cluster
- Isolamento total entre cliente e servidor
- Ideal para notebooks e desenvolvimento

### 5. Submeter via spark-submit (Modo Tradicional)

```bash
docker exec -it spark-master pyspark --master spark://spark-master:7077

docker exec -it spark-master spark-shell --master spark://spark-master:7077
```

## 📊 Configurações do Cluster

### Recursos dos Workers

Por padrão, cada worker tem:
- **Cores**: 2
- **Memória**: 2GB

Para ajustar, edite o `docker-compose.yml`:

```yaml
environment:
  - SPARK_WORKER_CORES=4
  - SPARK_WORKER_MEMORY=4g
```

### Delta Lake

O Delta Lake já está configurado no `spark-defaults.conf`:

```properties
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

### Apache Iceberg

O Iceberg está configurado com catálogo Hadoop:

```properties
spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type=hadoop
spark.sql.catalog.iceberg.warehouse=/data/iceberg-warehouse
```

**Usando Iceberg no código:**

```python
df.write.format("iceberg").saveAsTable("iceberg.namespace.tabela")

df = spark.table("iceberg.namespace.tabela")

spark.sql("CREATE TABLE iceberg.db.users (id INT, name STRING) USING iceberg")
```

## 🔍 Monitoramento

### Ver Workers Conectados

```bash
docker exec -it spark-master curl localhost:8080
```

### Ver Aplicações em Execução

```bash
docker exec -it spark-master curl localhost:4040/api/v1/applications
```

### Logs

```bash
docker exec -it spark-master cat /opt/spark/logs/spark-*.out

docker exec -it spark-worker-1 cat /opt/spark/logs/spark-*.out
```

## 🛑 Parar e Limpar

```bash
docker-compose down

docker-compose down -v

docker-compose down -v --rmi all
```

## 📝 Notas Importantes - Spark 4.0

1. **ANSI Mode**: Agora é padrão! Operações SQL são mais rigorosas
   - Divisão por zero lança exceção (não retorna NULL)
   - Overflow em operações numéricas lança exceção
   - Para desabilitar: `spark.sql.ansi.enabled=false`

2. **Java 17**: É o runtime padrão (Java 8 e 11 ainda suportados)

3. **Scala 2.13**: Versão padrão (2.12 ainda suportado)

4. **Delta Lake 4.0**: Inclui:
   - Delta Connect (integração com Spark Connect)
   - Liquid Clustering (melhor performance)
   - Coordinated Commits (multi-cloud)

5. **Apache Iceberg 1.7**: Inclui:
   - Partition Evolution (mudar particionamento sem reescrever)
   - Hidden Partitioning (usuário não vê partições)
   - Time Travel com snapshots
   - Multi-engine support (Spark, Flink, Trino, Presto)

6. **Spark Connect**: Cliente leve de apenas 1.5 MB vs 355 MB do PySpark completo

7. **Delta vs Iceberg**:
   - Use Delta Lake para ecosistema Databricks e máxima performance
   - Use Iceberg para multi-engine, neutralidade e partition evolution

8. Os eventos das aplicações são salvos em `/tmp/spark-events`
9. As Delta Tables são salvas em `/data/delta/` (persistente via volume)
10. As Iceberg Tables são salvas em `/data/iceberg-warehouse/` (persistente via volume)

## 🐛 Troubleshooting

**Spark Connect não conecta (fica travado):**

```bash
# 1. Verificar se o servidor Spark Connect está rodando
docker exec spark-master ps aux | grep connect

# Se NÃO estiver rodando, reconstruir a imagem:
docker-compose down
docker build -t spark-local-cluster:latest .
docker-compose up -d

# 2. Verificar logs
docker logs spark-master | grep -i connect

# 3. Testar conectividade
python test_connection.py

# 4. Iniciar manualmente se necessário
docker exec -it spark-master /opt/spark/sbin/start-connect-server.sh \
  --packages io.delta:delta-spark_2.13:4.0.1
```

**Workers não conectam ao Master:**
```bash
docker exec -it spark-worker-1 nc -zv spark-master 7077
```

**Aplicação não aparece no UI:**
```bash
docker exec -it spark-master ls -la /tmp/spark-events/
```

**Erro de memória:**
- Aumente `SPARK_WORKER_MEMORY` no docker-compose.yml
- Configure `--executor-memory` e `--driver-memory` no spark-submit

**Erro com ANSI mode:**
```bash
docker exec -it spark-master spark-submit \
  --conf spark.sql.ansi.enabled=false \
  --master spark://spark-master:7077 \
  /apps/seu_app.py
```

**Problemas com Java 17:**
- Spark 4.0 usa Java 17 por padrão
- Se tiver problemas, verifique compatibilidade das suas bibliotecas

## 📚 Recursos Adicionais

- [Spark 4.0 Release Notes](https://spark.apache.org/releases/spark-release-4-0-0.html)
- [Documentação Spark 4.0](https://spark.apache.org/docs/4.0.1/)
- [Delta Lake 4.0](https://docs.delta.io/)
- [Spark Connect Guide](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Migração para Spark 4.0](https://spark.apache.org/docs/4.0.1/migration-guide.html)