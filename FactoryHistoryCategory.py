import pandas as pd
import os
import pandas as pd
import json
import openai
import threading
import time
import re
import csv
from CommonFunctions import CommonFunctions

class FactoryHistoryCategory:

    def __init__(self, incremental_arquivo_csv, file_history_category_e_data, OpenAIkey):
        self.incremental_arquivo_csv = incremental_arquivo_csv 
        self.file_history_category_e_data = file_history_category_e_data
               
        self.ultimo_reset = time.time()  
        self.velocidade = 0
        self.contador_ultimo = 0
        self.OpenAIkey = OpenAIkey
        self.contador_atual = 0
        self.geral_pendencia_total = 0
        self.geral_total_itens_processados = 0
        pass

    prompt = """        
        Classifique as notícias do JSON abaixo em categorias como :
        - Policial
        - Esporte
        - Política
        - Entretenimento
        - Economia
        - Imprensa Marrom
        - Saúde
        - Tecnologia
        O JSON que estou enviando contém os campos history que é o ID e description que é a notícia.
        Me retorne uma lista JSON com a seguinte estrutura: 
        {
            "history": é o history que enviei na entrada, 
            "category": "Categoria da Noticia que você classificou "            
        }
        **IMPORTANTE:** **RETORNE APENAS UM JSON único JSON válido no formato de lista**. 
        NAO RESPONDA TEXTO ANTES DO JSON, NEM EXPLIQUE A SAÍDA!   
        """

    def categoriza_itens(self, pasta, qtde_batch, maxThreads):
        noticias_categorizadas = self.__retornar_noticias_ja_categorizadas()
        itens = self.__carrega_itens_processar(pasta, noticias_categorizadas)
        self.__processa_itens_gpt(itens, qtde_batch, maxThreads)
        self.__cria_noticias_categorizadas_com_data(pasta)

    def retorna_itens_processados(self):    
        
        dicionario = {}
        if os.path.exists(self.file_history_category_e_data): 
            df = pd.read_csv(self.file_history_category_e_data, usecols=[0, 1, 2])  # pega apenas a primeira coluna

              # Convertendo para dicionário indexado pela primeira coluna
            dicionario = {
                row[0]: {"history": row[0], "data": row[1],  "category": row[2]} for row in df.itertuples(index=False, name=None)
            }
            
        return dicionario
    
    
    

    def __retornar_noticias_ja_categorizadas(self):
        print(f"Iniciando Processamento , carregando Itens do diretorio \n")               
        noticias_categorizadas = {}
        category_limpo = {}

        # se existir o arquivo incremental carrega ele para descobrir quais noticias ja foram categorizadas
        # remove duplicatas e invalidas, senao cria vazio
        print(f"Validando Base de Itens para continuar processamento -->  {self.incremental_arquivo_csv}")

        if os.path.exists(self.incremental_arquivo_csv): 
            print(f"Base de Itens encontrada , iniciando validação da base")
            df_temp = pd.read_csv(self.incremental_arquivo_csv, usecols=[0, 1])  # pega apenas a primeira coluna
            df_temp.columns = ["history",  "category"]   
            dados_invalidos_encontrados = 0

            for index, (i, rowItem) in enumerate(df_temp.iterrows()):
                _history = rowItem['history']
                _category = rowItem['category'] 
                
                if CommonFunctions.validar_guid(_history) and _category and str(_category).strip() and str(_category).lower() != "empty" and str(_category).lower() != "nan" and str(_category).lower() != "none":                    
                    category_limpo[_history] = {"history" : _history, "category" : _category}
                else:                    
                    dados_invalidos_encontrados +=1

            # Sobrescreve CSV se foi encontrado alguma divergencia
            if dados_invalidos_encontrados > 0:
                print(f"\nIdentificado [{dados_invalidos_encontrados}] dados inválidos, sobrescrevendo arquivo para corrigir ...")
                lista_objetos = list(category_limpo.values())
                with open(self.incremental_arquivo_csv, "w", newline="", encoding="utf-8") as arquivo:
                    escritor = csv.DictWriter(arquivo, fieldnames=["history", "category"])
                    escritor.writeheader() 
                    escritor.writerows(lista_objetos)
                print(f"\nArquivo corrigido ! ")
            else:
                print(f"\nNenhum dado inválido foi encontrado na Base ! ")
            noticias_categorizadas = set(category_limpo.keys())            
        else:          
            with open(self.incremental_arquivo_csv, "w", encoding="utf-8") as arquivo:
                arquivo.write("history,category\n")
            print(f"Arquivo em branco foi criado ! \n")
        return noticias_categorizadas


    def __cria_noticias_categorizadas_com_data (self, pasta):
        print(f"Consolidando Itens ( Category e Data da noticia)  \n")        
        arquivos = [f for f in os.listdir(pasta) if f.endswith('.csv')]      
        dicionario_history = {}

        df_temp = pd.read_csv(self.incremental_arquivo_csv, usecols=[0, 1])  # pega apenas a primeira coluna
        df_temp.columns = ["history",  "category"]

        for index, (i, rowItem) in enumerate(df_temp.iterrows()):
                _history = rowItem['history']
                _category = rowItem['category']
                dicionario_history[_history] = {"history" : _history, "data" : "", "category": _category }

        for arquivo in arquivos:    
            print(f"Consolidando Itens do arquivo {arquivo}   \n")   

            caminho_completo = os.path.join(pasta, arquivo)
            df = pd.read_csv(caminho_completo, skiprows=0, sep=',', usecols=[0,2])
            df.columns = ["page",  "issued"] 

            for _, row in df.iterrows(): 
                history = row['page']
                data = row['issued']
                
                if history in dicionario_history.keys():
                    _category = dicionario_history[history]["category"]
                    dicionario_history[history] = {"history" : history, "data" : data, "category": _category }

        print(f"Criando arquivo consolidado {self.file_history_category_e_data} \n")   
        CommonFunctions.cria_csv_valores_dicionario(dicionario_history, self.file_history_category_e_data)                   
        

    def __carrega_itens_processar(self, pasta, noticias_categorizadas ):
        print(f"Iniciando Processamento , carregando Itens do diretorio \n")        
        arquivos = [f for f in os.listdir(pasta) if f.endswith('.csv')]      
        dicionario_history = {}

        # junta todos os arquivos antes de fazer a comparação  
        for arquivo in arquivos:     
            print(f"Carregando Itens para categorização - Carregando arquivo -->  {arquivo}")

            caminho_completo = os.path.join(pasta, arquivo)
            df = pd.read_csv(caminho_completo, skiprows=0, sep=',', usecols=[0,6])
            df.columns = ["page",  "caption"]  

            for _, row in df.iterrows(): 
                history = row['page']                
                # nao processa noticias ja categorizadas 
                if not history in noticias_categorizadas:                                     
                    if CommonFunctions.validar_guid(history):
                        dicionario_history[history] = {"history" : row['page'], "description": row['caption'] , "category" : "Empty"}
                else:
                    self.geral_total_itens_processados +=1            
        
        self.geral_pendencia_total = len(dicionario_history)
        print(f"\n [{self.geral_total_itens_processados}] Itens encontrados no CSV e já processados no arquivo : {self.incremental_arquivo_csv}\n")
        return dicionario_history

   
    def __processa_itens_gpt(self, dicionario_history, qtde_batch, maxThreads):        
        print(f"\nIniciando Processamento dos Itens... ")                  
       
        if len(dicionario_history) > 0:
            print(f"Iniciando cagetorização das noticias")                        

            threads = []

            for i, itens in enumerate(CommonFunctions.dividir_dicionario_em_lotes(dicionario_history, qtde_batch)):            
                #Processando os lotes e salvando no CSV em bateladas 
                thread = threading.Thread(target=self.__gpt_categorizar_noticias, args=(itens,i+1,  ))
                threads.append(thread)
                
            for idx, scheduled_threads in enumerate(CommonFunctions.dividir_lista_em_lotes(threads, maxThreads)):           
                for scheduled_thread in scheduled_threads:
                    scheduled_thread.start()

                for scheduled_thread in scheduled_threads:
                    scheduled_thread.join()                        
                    self.checa_velocidade()                               

            print(f"\nProcessamento concluído com Sucesso")
        else:
            print(f"\nNenhum novo Item para processar ")


    def __gpt_categorizar_noticias(self, noticias_dict, lote_ref):    
        try:
            msg = f"Lote {lote_ref} Processando noticias "   
            noticias_lista = [{"history": key, "description": value["description"]} for key, value in noticias_dict.items()]

            # Criar o prompt para a OpenAI
            prompt_composed = self.prompt

            for noticia in noticias_lista:
                prompt_composed += f'Entrada: {json.dumps(noticia, ensure_ascii=False)}\n'

            client = openai.OpenAI(api_key=self.OpenAIkey)       
            #model_choice = "gpt-3.5-turbo"
            model_choice = "gpt-4o-mini"

            response = client.chat.completions.create(
            model=model_choice,  
            messages=[{"role": "user", "content": prompt_composed}],
            temperature=0.2  
            )
            resposta_texto = response.choices[0].message.content   
            resposta_texto = re.sub(r"```json|```", "", resposta_texto).strip()
            noticias_categorizadas = json.loads(resposta_texto)

            if (len(noticias_categorizadas)) > 0:                
                df_lote = pd.DataFrame(noticias_categorizadas)                              
                df_lote.to_csv(self.incremental_arquivo_csv, index=False, sep=",", mode="a", header=False)
                self.contador_atual += len(noticias_categorizadas)
                msg+= f"| Arquivo incrementado com + {len(noticias_categorizadas)} Itens | Processamento Atual {self.contador_atual} "

            else:
                msg+= "| GPT não retornou item para atualizar nesse lote"
        
        except Exception as e:
            msg += f"| Ocorreu um erro: {e}"
            msg += f"\n resposta_texto {resposta_texto}"

            time.sleep(1)
        finally:            
            print(msg)
            

    def checa_velocidade(self):
        agora = time.time()
        if agora - self.ultimo_reset >= 60:  # Se passou 1 minuto, reseta                                    
            self.ultimo_reset = agora
            self.velocidade = self.contador_atual - self.contador_ultimo
            self.contador_ultimo = self.contador_atual
            horas = 0
            minutos = 0
            try:
                itens_processados = self.geral_total_itens_processados + self.contador_atual
                total_itens = self.geral_pendencia_total + self.geral_total_itens_processados
                percentual_processado = round(((itens_processados / total_itens) * 100),2)
                itens_restantes = total_itens - itens_processados            
                tempo_restante_minutos = itens_restantes / self.velocidade
                horas = int(tempo_restante_minutos // 60)
                minutos = int(tempo_restante_minutos % 60)
            except:
                pass
            finally:
                msg = f"\nProgresso: {percentual_processado} % [{itens_processados} de {total_itens}] | Itens/Min = {self.velocidade} | Tempo restante estimado: {horas} horas e {minutos} minutos.\n"
                print(msg) 
        


    