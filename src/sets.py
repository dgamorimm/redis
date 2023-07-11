from time import sleep
import redis
from typing import Any
from rich import print

r = redis.Redis()

# NÃO ORDENADOS

def s_inserir_elementos(chave:str, *args) -> int:
    return r.sadd(str(chave), *args)

def s_retornar_todos_elementos(chave:str) -> set:
    return r.smembers(str(chave))

def s_tamanho_conjunto(chave:str) -> int:
    return r.scard(str(chave))

def s_verificar_existencia(chave:str, elemento:str) -> bool:
    if r.sismember(str(chave), elemento):
        return '✅'
    return '❌'

def s_deletar_elementos(chave:str, *args) -> int:
    return r.srem(str(chave), *args)

def s_diferenca_entre_conjuntos(chave_1:str, chave_2:str) -> set:
    return r.sdiff(str(chave_1), str(chave_2))

def s_insterceccao_entre_conjuntos(chave_1:str, chave_2:str) -> set:
    return r.sinter(str(chave_1), str(chave_2))

# ORDENADOS

def z_inserir_elementos(chave:str, elementos:dict) -> int:
    # { VALOR : SCORE}
    return r.zadd(str(chave), elementos)

def z_tamanho_conjunto(chave:str) -> int:
    return r.zcard(str(chave))

def z_ranqueamento_elemento_p_score(chave:str, campo:str) -> int:
    return r.zrank(str(chave), campo)

def z_contar_elemento_range_score(chave:str, start_score:int, end_score:int) -> int:
    return r.zcount(str(chave), start_score, end_score)

def z_retornar_score_elemento(chave:str, campo:str) -> float:
    return r.zscore(str(chave), campo)

def z_retornar_elementos_range_score(chave:str, start_score:int, end_score:int, reverso:bool=False) -> list:
    return r.zrange(str(chave), start_score, end_score, reverso)

def z_deletar_elementos(chave:str, *args) -> int:
    return r.zrem(str(chave), *args)

if __name__ == '__main__':
    print(s_inserir_elementos(13, 'HADOOP', 'SPARK', 'HIVE', 'PIG'))
    print(s_inserir_elementos(13, 'FLUME', 'OOZIE', 'SOLR'))
    print(s_inserir_elementos(13, 'HADOOP'))
    print(s_retornar_todos_elementos(13))
    print(s_tamanho_conjunto(13))
    print(s_verificar_existencia(13, 'HADOOP'))
    print(s_verificar_existencia(13, 'SQL'))
    print(s_deletar_elementos(13, 'SPARK', 'OOZIE'))
    print(s_retornar_todos_elementos(13))
    print(s_inserir_elementos(14, 'HIVE', 'HADOOP', 'SPARK'))
    print(s_diferenca_entre_conjuntos(13,14))
    print(s_diferenca_entre_conjuntos(14,13))
    print(s_insterceccao_entre_conjuntos(14,13))
    
    print('\n\n')
    
    elementos = {'NOSQL' : 0, 'RELACIONAL' : 5, 'DIMENSIONAL' : 4}
    print(z_inserir_elementos(35, elementos))
    print(z_tamanho_conjunto(35))
    print(z_ranqueamento_elemento_p_score(35, 'DIMENSIONAL'))
    print(z_contar_elemento_range_score(35, 0, 5))
    print(z_contar_elemento_range_score(35, 0, 4))
    print(z_retornar_score_elemento(35, 'DIMENSIONAL'))
    print(z_retornar_elementos_range_score(35, 0, 5, False))
    print(z_retornar_elementos_range_score(35, 0, 5, True))
    print(z_deletar_elementos(35, 'DIMENSIONAL'))