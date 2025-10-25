"""
Script para testar conectividade com Spark Connect
"""
import socket
import sys

def test_port(host, port):
    """Testa se uma porta está aberta"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    result = sock.connect_ex((host, port))
    sock.close()
    return result == 0

print("=" * 70)
print("Testando conectividade com o cluster Spark...")
print("=" * 70)

ports_to_test = {
    "Spark Master UI": 8080,
    "Spark Master": 7077,
    "History Server": 18080,
    "Spark Connect": 15002,
    "Application UI": 4040
}

print("\nTestando portas no localhost:\n")
all_ok = True

for service, port in ports_to_test.items():
    if test_port("localhost", port):
        print(f"✓ {service:20s} (porta {port}): CONECTADO")
    else:
        print(f"✗ {service:20s} (porta {port}): FALHOU")
        all_ok = False

print("\n" + "=" * 70)

if not test_port("localhost", 15002):
    print("\n⚠️  Spark Connect não está acessível!")
    print("\nPossíveis soluções:\n")
    print("1. Verificar se o container está rodando:")
    print("   docker ps | grep spark-master")
    print("\n2. Verificar logs do container:")
    print("   docker logs spark-master")
    print("\n3. Verificar se o Spark Connect iniciou:")
    print("   docker exec spark-master ps aux | grep connect")
    print("\n4. Reconstruir a imagem com o fix:")
    print("   docker-compose down")
    print("   docker build -t spark-local-cluster:latest .")
    print("   docker-compose up -d")
    print("\n5. Verificar portas mapeadas:")
    print("   docker port spark-master")
    sys.exit(1)
else:
    print("\n✓ Spark Connect está acessível!")
    print("\nVocê pode rodar:")
    print("  python spark_connect_local.py")
    print("\n" + "=" * 70)

print("\nTestando conexão Spark Connect...\n")

try:
    from pyspark.sql import SparkSession
    
    print("Conectando ao Spark Connect...")
    spark = SparkSession.builder \
        .remote("sc://localhost:15002") \
        .appName("TestConnection") \
        .config("spark.connect.grpc.deadline", "10s") \
        .getOrCreate()
    
    print(f"✓ Conectado com sucesso!")
    print(f"  Spark Version: {spark.version}")
    
    df = spark.range(5)
    count = df.count()
    print(f"  Teste executado: range(5).count() = {count}")
    
    spark.stop()
    print("\n✓ Todos os testes passaram!")
    
except ImportError:
    print("✗ PySpark não está instalado!")
    print("\nInstale com: pip install pyspark==4.0.1 delta-spark==4.0.1")
    
except Exception as e:
    print(f"✗ Erro ao conectar: {e}")
    print("\nVerifique os logs: docker logs spark-master")

print("=" * 70)