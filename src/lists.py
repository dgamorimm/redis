from time import sleep
import redis
from typing import Any
from rich import print

r = redis.Redis()

def inserir_inicio_lista(chave:str, lista:list) -> int:
    return r.lpush(str(chave), *lista)

def inserir_final_lista(chave:str, lista:list) -> int:
    return r.rpush(str(chave), *lista)

def retornar_lista_range(chave:str, start:int, end:int) -> list:
    return r.lrange(str(chave), start, end)

def adicionar_item_lista_posicao(chave:str, momento:str, item_posicao:str, valor_adicionar:Any) -> int:
    if momento == 'depois':
        return r.linsert(str(chave), 'after', item_posicao, valor_adicionar)
    return r.linsert(str(chave), 'before', item_posicao, valor_adicionar)

def deletar_toda_lista(chave:str) -> bool:
    return r.delete(str(chave))

def atualizar_item_lista(chave:str, indice:int, valor:Any) -> bool:
    return r.lset(str(chave), indice, valor)

def retornar_valor_indice(chave:str, indice:int) -> bytes:
    return r.lindex(str(chave), indice)

def retornar_tamanho_lista(chave:str) -> int:
    return r.llen(str(chave))

def deletar_valor_inicio(chave:str) -> bytes:
    return r.lpop(str(chave))

def deletar_valor_final(chave:str) -> bytes:
    return r.rpop(str(chave))

if __name__ == '__main__':
    print(inserir_inicio_lista(4545, ['SQLSERVER','ORCACLE', 'POSTGRES', 'MYSQL']))
    print(inserir_final_lista(4545, ['DB2']))
    print(retornar_lista_range(4545, 0,3))
    print(retornar_lista_range(4545, 0,4))
    print(adicionar_item_lista_posicao(chave=4545,momento='depois',item_posicao='ORCACLE', valor_adicionar='FIREBIRD'))
    print(adicionar_item_lista_posicao(chave=4545,momento='antes',item_posicao='FIREBIRD', valor_adicionar='SQLITE'))
    print(atualizar_item_lista(chave=4545, indice=1, valor='POSTGRESQL'))
    print(retornar_valor_indice(chave=4545, indice=0))
    print(retornar_valor_indice(chave=4545, indice=3))
    print(retornar_tamanho_lista(chave=4545))
    print(deletar_valor_inicio(chave=4545))
    print(deletar_valor_final(chave=4545))
    print(retornar_lista_range(4545, 0,-1))  # ver toda a lista
    print(deletar_toda_lista(4545))