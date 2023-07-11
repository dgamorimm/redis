from time import sleep
import redis
from typing import Any

r = redis.Redis()

def inserir_valor(chave:Any,valor: Any,expire:int = None, expire_time:str = 'sec') -> bool:
    if expire:
        r.set(str(chave), valor)
        if expire_time == 'sec':
            r.expire(str(chave), expire)  # segundos
        else:
            r.pexpire(str(chave), expire)  # milisegundos
        return '✅'
    r.set(str(chave), valor)
    return '✅'

def retornar_valor(chave:Any) -> str:
    return r.get(str(chave)).decode()

def inserir_varias_chaves(dicionario:dict) -> bool:
    return r.mset(dicionario)

def verificar_chave(chave:Any) -> str:
    if r.exists(str(chave)):
        return '✅'
    return '❌'

def deletar_valor(chave:Any) -> bool:
    return r.delete(str(chave))

def verificar_tipo(chave:Any) -> str:
    return r.type(str(chave)).decode()

def tempo_que_resta_expiracao(chave:Any, expire_time:str = 'sec' ) -> int:
    if expire_time == 'sec':
        return r.ttl(str(chave))
    return r.pttl(str(chave))

def remover_tempo_expiracao(chave:Any) -> bool:
    return r.persist(str(chave))

def retornar_substring_valor(chave:Any, start:int, end:int) -> str:
    return r.getrange(str(chave),start, end).decode()

def atualizar_registro(chave:Any,valor: Any) -> str:
    return r.getset(str(chave), valor).decode()

def retornar_valores(chaves:list) -> list:
    return list(map(lambda x : x.decode() if x else x, r.mget(*chaves)))

def tamanho_valor_chave(chave:Any) -> int:
    return r.strlen(str(chave))

if __name__ == '__main__' :
    print(inserir_valor(1, 'ENGENHEIRO DE DADOS'))
    print(retornar_valor(1))
    dicionario = {
        "1040": "ANALISTA",
        "1050": "GERENTE",
        "1060": "TESTADOR"
    }
    print(inserir_varias_chaves(dicionario))
    print(verificar_chave(1))
    print(verificar_chave(455))
    print(verificar_chave(1040))
    print(deletar_valor(1040))
    print(verificar_tipo(1050))
    print(inserir_valor(2, 'ENGENHEIRO CIVIL', 60))
    print(inserir_valor(3, 'ENGENHEIRO MECANINCO', 885000, 'ms'))
    # sleep(10)
    print(tempo_que_resta_expiracao(2))
    print(tempo_que_resta_expiracao(3, 'ms'))
    print(remover_tempo_expiracao(3))
    print(tempo_que_resta_expiracao(3, 'ms'))
    print(inserir_valor(1020, 'ENGENHEIRO DE DADOS'))
    print(retornar_valor(1020))
    print(retornar_substring_valor(1020,0,9))
    print(atualizar_registro(1, 'ENGENHEIRO CIVIL'))
    print(retornar_valores(['1020','1', '2', '455']))
    print(tamanho_valor_chave(1020))