from time import sleep
import redis
from typing import Any
from rich import print

r = redis.Redis()

def inserir_hash(chave:Any,elementos:dict) -> bool:
    for key, value in elementos.items():
        r.hset(str(chave),key,value)
    return '✅'

def deletar_hash(chave:str, campo:str) -> bool:
    return r.hdel(str(chave),str(campo))

def retornar_todos_campos_hash(chave:str) -> dict:
    return r.hgetall(str(chave))

def retornar_por_campo(chave:str, *args) -> list:
    return r.hmget(str(chave), *args)

def retornar_todos_campos_hash2(chave:str) -> list:
    return r.hvals(str(chave))

def verificar_existencia_campo(chave:str, campo:str) -> str:
    if r.hexists(str(chave),campo):
        return '✅'
    return '❌'

def quantidade_campos(chave:str) -> int:
    return r.hlen(str(chave))

def retorna_campos(chave:str) -> list:
    return r.hkeys(str(chave))

if __name__ == '__main__':
    elementos ={
        'NOME' : 'DOUGLAS',
        'PROFISSAO' : 'ENGENHEIRO DE DADOS',
        'CIDADE' : 'RIO DE JANEIRO'
    }
    print(inserir_hash(chave='CADASTRO', elementos=elementos))
    
    print(deletar_hash(chave='CADASTRO', campo='CIDADE'))
    print(retornar_todos_campos_hash(chave='CADASTRO'))
    print(retornar_por_campo('CADASTRO',['NOME', 'PROFISSAO']))
    print(retornar_todos_campos_hash2(chave='CADASTRO'))
    print(verificar_existencia_campo(chave='CADASTRO', campo='NOME'))
    print(verificar_existencia_campo(chave='CADASTRO', campo='NOMES'))
    print(quantidade_campos(chave='CADASTRO'))
    print(retorna_campos(chave='CADASTRO'))