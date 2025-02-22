import os
import pandas as pd
import time
from CommonFunctions import CommonFunctions
    
from collections import Counter

class FactoryHistoryRanking:

    def __init__(self, arquivo_csv_ranking, arquivo_csv_user_history, arquivo_csv_user_preference):
        self.arquivo_csv_ranking = arquivo_csv_ranking
        self.arquivo_csv_user_history = arquivo_csv_user_history
        self.arquivo_csv_user_preference = arquivo_csv_user_preference
        
        self.ultimo_print = time.time()
        self.ultimo_reset = time.time()  
        self.geral_total_itens_processados = 0
        self.geral_total_itens = 0
        pass




    def processa_ranking(self, pasta,  history_category_dictionary:dict):        
        arquivos = [f for f in os.listdir(pasta) if f.endswith('.csv')]          
        dict_history = {}
        dict_usuario_history = {}

        try:
            # Lê cada arquivo e concatena
            print(f"Iniciando Processamento do Ranking , concatenação de categoria e classificacão de gosto do usuario\n")

            for arquivo in arquivos:                         
                caminho_completo = os.path.join(pasta, arquivo)
                df = pd.read_csv(caminho_completo, skiprows=0, sep=",")        
                df.head()
                cols = df.columns   

                self.__zera_progresso(len(df.values))                
                print(f"Arquivo {arquivo} Itens: {len(df.values)}")

                for i, values in enumerate(df.values):   
                    try:                        
                        self.__incrementa_progresso()
                        self.__checa_progresso()

                        userId = values[cols.get_loc("userId")]                  
                        historySize = values[cols.get_loc("historySize")]           
                        
                        # Converter a string para uma lista mantendo a ordem original
                        l_history = [valor.strip() for valor in values[cols.get_loc("history")].split(",")]
                        #l_timestampHistory = [valor.strip() for valor in values[cols.get_loc("timestampHistory")].split(",")]
                        #l_numberOfClicksHistory = [valor.strip() for valor in values[cols.get_loc("numberOfClicksHistory")].split(",")]
                        #l_timeOnPageHistory = [valor.strip() for valor in values[cols.get_loc("timeOnPageHistory")].split(",")]
                        #l_scrollPercentageHistory = [valor.strip() for valor in values[cols.get_loc("scrollPercentageHistory")].split(",")]
                        l_pageVisitsCountHistory = [valor.strip() for valor in values[cols.get_loc("pageVisitsCountHistory")].split(",")]

                        #if not all(item == historySize for item in [len(l_history), len(l_timestampHistory),len(l_numberOfClicksHistory), len(l_timeOnPageHistory),len(l_scrollPercentageHistory),len(l_pageVisitsCountHistory)]):
                        #    next
                        
                        if not historySize == len(l_pageVisitsCountHistory):
                            next
                        
                        # Usando enumerate para obter o índice
                        for index, hist_item in enumerate(l_history):
                            try:
                                if not CommonFunctions.validar_guid(hist_item):
                                    break

                                if userId in dict_usuario_history.keys():
                                    if not hist_item in dict_usuario_history[userId]:
                                        dict_usuario_history[userId].append(hist_item)
                                else:
                                    dict_usuario_history[userId] = []
                                    dict_usuario_history[userId].append(hist_item)                                    

                                data = ""
                                category = "None"
                                if hist_item in history_category_dictionary.keys():
                                    category = history_category_dictionary[hist_item]["category"]
                                    data = history_category_dictionary[hist_item]["data"]

                                #objeto_novo = self.__cria_objeto_ranking(hist_item, category, data,   l_timestampHistory[index], l_numberOfClicksHistory[index],   l_timeOnPageHistory[index],   l_scrollPercentageHistory[index], l_pageVisitsCountHistory[index] )
                                pageVisitsCountHistory = 0
                                if CommonFunctions.is_integer(l_pageVisitsCountHistory[index]):
                                    pageVisitsCountHistory = int(l_pageVisitsCountHistory[index])

                                objeto_novo = {"history" :hist_item, "pageVisitsCountHistory" :pageVisitsCountHistory, "data" : data,  "category" : category }

                                if hist_item not in dict_history:
                                    dict_history[hist_item] = objeto_novo                                
                                else:   
                                    # checa se existe category, se existir atualiza o objeto                                    
                                    dict_history[hist_item]["category"] = category
                                    dict_history[hist_item]["data"] = data
                                    #dict_history[hist_item]["timeOnPageHistory"] =  ( int(dict_history[hist_item]["timeOnPageHistory"]) + int(objeto_novo["timeOnPageHistory"]))
                                    #dict_history[hist_item]["numberOfClicksHistory"] = ( int(dict_history[hist_item]["numberOfClicksHistory"]) + int(objeto_novo["numberOfClicksHistory"]))
                                    #dict_history[hist_item]["scrollPercentageHistory"] = round((float(dict_history[hist_item]["scrollPercentageHistory"]) + float(objeto_novo["scrollPercentageHistory"])),2)
                                    dict_history[hist_item]["pageVisitsCountHistory"] = (int(dict_history[hist_item]["pageVisitsCountHistory"]) + int(objeto_novo["pageVisitsCountHistory"]))
                                    #if int(objeto_novo["timestampHistory"]) > int(dict_history[hist_item]["timestampHistory"]):
                                    #    dict_history[hist_item]["timestampHistory"] = int(objeto_novo["timestampHistory"])       

                            except Exception as e:
                                next
                        
                    except Exception as e:
                        next     
                print(f"Arquivo {arquivo} Concluido\n")                         
        
            print(f"Criando arquivos...")
            self.__cria_csv_user_preferencias(dict_usuario_history, dict_history)            
            self.__cria_csv_history_ranking(dict_history)            
            self.__cria_csv_user_history(dict_usuario_history)            
            print(f"Arquivos Salvo com Sucesso")
        except Exception as e:
            print(f"Erro: {e}") 

    def __cria_objeto_ranking(self, history, category, data,  timestampHistoryValue, numberOfClicksHistoryValue,  timeOnPageHistoryValue, scrollPercentageHistoryValue,  pageVisitsCountHistoryValue):
        timestampHistory = 0
        numberOfClicksHistory  = 0
        timeOnPageHistory  = 0
        scrollPercentageHistory = 0.0
        pageVisitsCountHistory  = 0

        if CommonFunctions.is_integer(timestampHistoryValue):
            timestampHistory = int(timestampHistoryValue)

        if CommonFunctions.is_integer(numberOfClicksHistoryValue):
            numberOfClicksHistory = int(numberOfClicksHistoryValue)
        
        if CommonFunctions.is_integer(timeOnPageHistoryValue):
            timeOnPageHistory = int(timeOnPageHistoryValue)

        if CommonFunctions.is_integer(scrollPercentageHistoryValue):
            scrollPercentageHistory = float(scrollPercentageHistoryValue)

        if CommonFunctions.is_integer(pageVisitsCountHistoryValue):
            pageVisitsCountHistory = int(pageVisitsCountHistoryValue)
        
        objeto_novo = {"history" :history, 
                       #"timestampHistory" : timestampHistory,  
                       #"numberOfClicksHistory" : numberOfClicksHistory , 
                       #"timeOnPageHistory" : timeOnPageHistory, 
                       #"scrollPercentageHistory" : scrollPercentageHistory , 
                       "pageVisitsCountHistory" :pageVisitsCountHistory,
                       "data" : data, 
                       "category" : category
                    }
        
        return objeto_novo

    def __cria_csv_history_ranking(self,dict_history:dict):
        CommonFunctions.cria_csv_valores_dicionario(dict_history, self.arquivo_csv_ranking)
        
    def __cria_csv_user_history(self, dict_usuario_history:dict ):
        lista_unica = []
        for user_id, lista in dict_usuario_history.items():
            for item in lista:                
                lista_unica.append({"userId": user_id, "history" : item })
        
        df = pd.DataFrame(lista_unica)            
        df.to_csv(self.arquivo_csv_user_history, index=False, encoding="utf-8")
        

    def __cria_csv_user_preferencias(self, dict_usuario_history:dict, dict_history:dict):
        user_favorite_category = {}
        # Iterar pelos usuários e contar quantas vezes cada categoria foi acessada
        for user, history_list in dict_usuario_history.items():                
            cats = {chave: dict_history[chave] for chave in history_list if chave in dict_history}
            categorias = []
            for idx, item in cats.items():
                categorias.append(item["category"])                
            contagem = Counter(categorias)
            categoria_mais_acessada = contagem.most_common(1)
            
            if categoria_mais_acessada:
                user_favorite_category[user] = categoria_mais_acessada[0][0]  # Pega o nome da categoria

        df = pd.DataFrame(user_favorite_category.items(), columns=["userId", "category"])
        df.to_csv(self.arquivo_csv_user_preference, index=False, encoding="utf-8")
        
    
    def __zera_progresso(self, geral_total_itens):        
        self.geral_total_itens_processados = 0
        self.geral_total_itens = geral_total_itens
               
    def __incrementa_progresso(self,  ):        
        self.geral_total_itens_processados +=1
        

    def __checa_progresso(self):
        agora = time.time()        
        try:
            itens_processados = self.geral_total_itens_processados
            total_itens = self.geral_total_itens
            percentual_processado = 0
            percentual_processado = round(((itens_processados / total_itens) * 100),2)
            
        except:
            pass
        finally:
            if agora - self.ultimo_print >=5:
                self.ultimo_print = agora
                msg = f"Progresso: {percentual_processado} % [{itens_processados} de {total_itens}]"
                print(msg)
            
        

