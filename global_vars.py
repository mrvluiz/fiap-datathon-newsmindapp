import pandas as pd
import json
import os


class GlobalData:
    """Classe para armazenar e manipular um DataFrame global."""
    
    def __init__(self):
        self.first_run = True
        self.df_history_ranking = pd.DataFrame(json.loads("{}"))
        self.df_user_x_history = pd.DataFrame(json.loads("{}"))
        self.df_user_x_preferences = pd.DataFrame(json.loads("{}"))
        self.g_OPENAI_KEY = os.getenv("OPENAI_KEY")
        self.g_GLOBO_DIR_ITENS = os.getenv("GLOBO_DIR_ITENS")
        self.g_GLOBO_DIR_TREINO = os.getenv("GLOBO_DIR_TREINO")
        self.g_FILE_HISTORY_CATEGORY = os.getenv("FILE_HISTORY_CATEGORY")
        self.g_FILE_HISTORY_RANKING = os.getenv("FILE_HISTORY_RANKING")
        self.g_FILE_USER_X_HISTORY = os.getenv("FILE_USER_X_HISTORY")
        self.g_FILE_USER_X_PREFERENCES = os.getenv("FILE_USER_X_PREFERENCES")
        self.g_FILE_HISTORY_CATEGORY_E_DATA = os.getenv("FILE_HISTORY_CATEGORY_E_DATA")
    
    def carregar_csv(self, force_load): 
        
        self.g_OPENAI_KEY = os.getenv("OPENAI_KEY")
        self.g_GLOBO_DIR_ITENS = os.getenv("GLOBO_DIR_ITENS")
        self.g_GLOBO_DIR_TREINO = os.getenv("GLOBO_DIR_TREINO")
        self.g_FILE_HISTORY_CATEGORY = os.getenv("FILE_HISTORY_CATEGORY")
        self.g_FILE_HISTORY_RANKING = os.getenv("FILE_HISTORY_RANKING")
        self.g_FILE_USER_X_HISTORY = os.getenv("FILE_USER_X_HISTORY")
        self.g_FILE_USER_X_PREFERENCES = os.getenv("FILE_USER_X_PREFERENCES")
        self.g_FILE_HISTORY_CATEGORY_E_DATA = os.getenv("FILE_HISTORY_CATEGORY_E_DATA")

        if force_load == True or self.first_run == True:
            try:
                if os.path.exists(self.g_FILE_HISTORY_RANKING):
                    dtypes_history_ranking = {"history": "category","pageVisitsCountHistory": "int32","category": "category","data": "string"}
                    self.df_history_ranking = pd.read_csv(self.g_FILE_HISTORY_RANKING, dtype=dtypes_history_ranking, parse_dates=["data"])

                if os.path.exists(self.g_FILE_USER_X_HISTORY):
                    dtypes_user_x_history = {"userId": "category","history": "category"}
                    self.df_user_x_history = pd.read_csv(self.g_FILE_USER_X_HISTORY, dtype=dtypes_user_x_history)      

                if os.path.exists(self.g_FILE_USER_X_PREFERENCES ):
                    dtypes_user_x_preferences = {"userId": "category","category": "category"} 
                    self.df_user_x_preferences = pd.read_csv(self.g_FILE_USER_X_PREFERENCES, dtype=dtypes_user_x_preferences)        
                self.first_run = False
            except:
                  pass


# Criando uma instância global da classe
gv = GlobalData()