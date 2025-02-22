import uuid
import pandas as pd

class CommonFunctions:
      
    @staticmethod
    def validar_guid(valor):
        try:
            uuid_obj = uuid.UUID(valor, version=4)  
            return True
        except ValueError:
            return False

    @staticmethod   
    def dividir_lista_em_lotes( lista, tamanho_lote):        
        for i in range(0, len(lista), tamanho_lote):
            yield lista[i:i + tamanho_lote]

    @staticmethod
    def dividir_dicionario_em_lotes(dicionario, tamanho_lote=100):        
        chave_lista = list(dicionario.keys())
        for i in range(0, len(chave_lista), tamanho_lote):            
            yield {chave: dicionario[chave] for chave in chave_lista[i:i + tamanho_lote]}

    @staticmethod
    def is_integer(s):
        try:
            int(s)  # Tenta converter para inteiro
            return True
        except ValueError:
            try:
                float(s)  # Se falhar, tenta converter para float
                return True
            except ValueError:
                return False
        except:
            return False
    
    @staticmethod
    def cria_csv_valores_dicionario(dictionary:dict, file_name_csv):
        lista_objetos = list(dictionary.values())
        df = pd.DataFrame.from_dict(lista_objetos)        
        df.to_csv(file_name_csv, index=False, encoding="utf-8")