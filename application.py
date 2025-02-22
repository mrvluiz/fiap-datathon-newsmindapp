import os
import json
import pandas as pd
from flask import Flask, request, jsonify, Response
from flask_restx import Api, Resource, fields

from FactoryHistoryRanking import FactoryHistoryRanking
from FactoryHistoryCategory import FactoryHistoryCategory
from MLRecomendacao import MLRecomendacao

import global_vars 


# Inicializar o Flask e Prometheus
app = Flask(__name__)
app.json.sort_keys = False
api = Api(app,
            version='1.0.0',
            title='API TECH 5 @mrvluiz',
            description='API para previsibilidade de noticias da globo.com',
            default='Principal'
            )



# Modelo de entrada (para documentação)
input_model = api.model('Predicao Model', {
    'userId': fields.String(required=True, description="Id do usuario para predição", default="c33bcbcaf32fe895fb0a854c94531120657ad5adabe3e14b57fc4b4167074906")
})

@api.route("/predicao/", methods=['POST'])
@api.doc(description="Retorna a previsão noticias de acordo com o UserId informado")
class Preditiva(Resource):        
        @api.expect(input_model)
        def post(self):                  
                # Obter os parâmetros do corpo da requisição
                try:
                    global_vars.gv.carregar_csv(force_load=False)
                     
                    data = request.get_json()
                    userId = data.get('userId')
                    
                    # Verificar se os parâmetros foram enviados
                    if not userId :
                        return {"error": "O campo 'userId' é obrigatório."}, 400

                    MLrecomenda = MLRecomendacao()
                    predicao_retorno = MLrecomenda.executar_modelo(userId, global_vars.gv.df_history_ranking, global_vars.gv.df_user_x_history,  global_vars.gv.df_user_x_preferences )

                    return {"status": "Sucesso" , "mensagem": predicao_retorno}, 200
                except Exception as e:
                    return {"status": "Erro" , "mensagem": f"Erro: {e}"}, 400                


@api.route("/factory-files/", methods=['POST'])
@api.doc(description="Cria os arquivos base de predição com base nos dados historicos da globo")
class Preditiva(Resource):  
        def post(self):                  
                # Obter os parâmetros do corpo da requisição               
                try:
                    global_vars.gv.carregar_csv(force_load=True)   

                    factory_category = FactoryHistoryCategory(global_vars.gv.g_FILE_HISTORY_CATEGORY, global_vars.gv.g_FILE_HISTORY_CATEGORY_E_DATA, global_vars.gv.g_OPENAI_KEY)
                    factory_category.categoriza_itens(global_vars.gv.g_GLOBO_DIR_ITENS, 25, 10)
                    category_itens = factory_category.retorna_itens_processados()
                    factory_hist_rank = FactoryHistoryRanking(global_vars.gv.g_FILE_HISTORY_RANKING, global_vars.gv.g_FILE_USER_X_HISTORY, global_vars.gv.g_FILE_USER_X_PREFERENCES)
                    factory_hist_rank.processa_ranking(global_vars.gv.g_GLOBO_DIR_TREINO,category_itens)
                    
                    return {"status": "Sucesso" , "mensagem": "Arquivos processados"}, 200
                except Exception as e:
                    msg_complementar = f"GLOBO_DIR_ITENS[{global_vars.gv.g_GLOBO_DIR_ITENS}] GLOBO_DIR_TREINO[{global_vars.gv.g_GLOBO_DIR_TREINO}] FILE_HISTORY_CATEGORY[{global_vars.gv.g_FILE_HISTORY_CATEGORY}]"
                    return {"status": "Erro" , "mensagem": f"Erro: {e} msg_complementar {msg_complementar}" }, 400
        
                
if __name__ == '__main__':   
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)







 




