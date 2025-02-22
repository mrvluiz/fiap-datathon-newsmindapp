import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.impute import SimpleImputer
import joblib
import os


class MLRecomendacao:

    def __init__(self):           
        pass
    
    def __criar_modelo(self, modelo_file,  df):        
        if not os.path.exists(modelo_file): 

            # Selecionar as features para treinar o modelo
            features = ["pageVisitsCountHistory"]
            X = df[features]
            y = df["category"]  # Target: categoria favorita
                        
            imputer = SimpleImputer(strategy='mean')  # Replace NaN with the mean of the column
            X = imputer.fit_transform(X)

            
            print(f"Dividir entre treino e teste")
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

            print(f"Treinar um modelo de Random Forest") 
            modelo = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
            modelo.fit(X_train, y_train)

            print(f"Execute predict") 
            y_pred = modelo.predict(X_test)
            acuracia = accuracy_score(y_test, y_pred)

            print(f"Acurácia do modelo: {acuracia:.4f}")
            print("Relatório de Classificação:\n", classification_report(y_test, y_pred))

            # Salvar o modelo treinado
            joblib.dump(modelo, modelo_file)
            print("Modelo salvo com sucesso!")
        else:             
            print("Modelo ja existente!")


    def __recomendar_noticias(self, user_id, modelo_file, df_user_x_history, df_history_ranking, df_user_x_preferences):
        # Se o userId já estiver na base
        if user_id in df_user_x_history["userId"].values:

            print(f"Carregando Modelo para usuario existente") 
            modelo_carregado = joblib.load(modelo_file)  

            # Noticias ja lidas
            historico = df_user_x_history[df_user_x_history["userId"] == user_id]["history"].tolist()

            print(f"categoria favorita do usuário")  
            categoria_preferida = df_user_x_preferences[df_user_x_preferences["userId"] == user_id]["category"].values[0]
            
            print(f"Filtrar notícias que pertencem à categoria preferida do usuário")  
            noticias_candidatas = df_history_ranking[df_history_ranking["category"] == categoria_preferida]

            print(f"Remover notícias que o usuário já viu") 
            noticias_candidatas = noticias_candidatas[~noticias_candidatas["history"].isin(historico)]

            print(f"prever quais notícias são mais prováveis de interesse")  
            X_noticias = noticias_candidatas[["pageVisitsCountHistory"]]
            predicoes = modelo_carregado.predict_proba(X_noticias)

            print(f"Ordenar notícias com maior probabilidade de interesse")  
            noticias_candidatas["score"] = predicoes[:, 1]  
            recomendacoes = noticias_candidatas.sort_values(by="score", ascending=False)

            return recomendacoes["history"].tolist()[:10]  # Retorna top 10 noticias

        else:
            print(f"Indicando noticia para um usuario novo")   
            top_categorias = df_history_ranking["category"].value_counts().index[:3]
            noticias_populares = df_history_ranking[df_history_ranking["category"].isin(top_categorias)]
            recomendacoes = noticias_populares.sort_values(by="data", ascending=False)
            return recomendacoes["history"].tolist()[:10] 


    def executar_modelo(self, user_id_teste, df_history_ranking, df_user_x_history, df_user_x_preferences):

        df_history_ranking.columns = df_history_ranking.columns.str.strip().str.replace(r"[^\w]", "", regex=True)
        df_user_x_preferences.columns = df_user_x_preferences.columns.str.strip().str.replace(r"[^\w]", "", regex=True)

        df_history_ranking_unique  = df_history_ranking.sort_values(
            by="pageVisitsCountHistory", ascending=False
        ).drop_duplicates(subset=["category"])

        df_user_x_preferences["pageVisitsCountHistory"] = df_user_x_preferences["category"].map(
            df_history_ranking_unique.set_index("category")["pageVisitsCountHistory"]
        )
        df_user_x_preferences["latest_news_date"] = df_user_x_preferences["category"].map(
            df_history_ranking_unique.set_index("category")["data"]
        )

        modelo_file = "modelo_recomendacao.pkl"
        self.__criar_modelo(modelo_file, df_user_x_preferences )            
        recomendacoes = self.__recomendar_noticias(user_id_teste, modelo_file, df_user_x_history, df_history_ranking, df_user_x_preferences)
        print("Notícias recomendadas:", recomendacoes)
        return recomendacoes