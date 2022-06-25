
# Bibliotecas
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('sqlite:///tera.db3', echo=False)
pd.set_option('display.max_colwidth', 150)


# Coleta e tratamento de dados
def busca_cadastro_cvm():
    try:
        url = 'http://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv'
        return pd.read_csv(url, sep=';', encoding='ISO-8859-1',low_memory=False)
    except:
        print("Arquivo não encontrado")

def busca_informes_diarios_CVM(data_inicio, data_fim):
    datas = pd.date_range(data_inicio, data_fim, freq='MS')
    informe_completo = pd.DataFrame()
    informe_mensal = pd.DataFrame()

    for data in datas:
        try:
            url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{}{:02d}.zip'.format(data.year, data.month)
            print(url)
            informe_mensal = pd.read_csv(url, sep=';', low_memory=False)
        except:
            print("Arquivo não encontrado")

        informe_completo = pd.concat([informe_completo, informe_mensal], ignore_index=True)

    return informe_completo

#função para consultar fundos
def consulta_fundo(informes, cnpj):  
    fundo = informes[informes['CNPJ_FUNDO'] == cnpj].copy()
    fundo.set_index('DT_COMPTC', inplace=True)
    fundo['cotas_normalizadas'] = fundo['VL_QUOTA'] / fundo['VL_QUOTA'].iloc[0]
    return fundo

#retorna melhores e piores fundos
def melhores_e_piores(informes, cadastro, top=5, minimo_de_cotistas=100, classe=''):  
    cadastro = cadastro[cadastro['SIT'] == 'EM FUNCIONAMENTO NORMAL']
    fundos = informes[informes['NR_COTST'] >= minimo_de_cotistas]
    cnpj_informes = fundos['CNPJ_FUNDO'].drop_duplicates()
    
    fundos = fundos.pivot_table(index='DT_COMPTC', columns='CNPJ_FUNDO')  
    cotas_normalizadas = fundos['VL_QUOTA'] / fundos['VL_QUOTA'].iloc[0]
    
    if classe == 'multimercado':
        cnpj_cadastro = cadastro[cadastro['CLASSE'] == 'Fundo Multimercado']['CNPJ_FUNDO']   
        cotas_normalizadas = cotas_normalizadas[cnpj_cadastro[cnpj_cadastro.isin(cnpj_informes)]]

    if classe == 'acoes':
        cnpj_cadastro = cadastro[cadastro['CLASSE'] == 'Fundo de Ações']['CNPJ_FUNDO']   
        cotas_normalizadas = cotas_normalizadas[cnpj_cadastro[cnpj_cadastro.isin(cnpj_informes)]]

    if classe == 'rendafixa':
        cnpj_cadastro = cadastro[cadastro['CLASSE'] == 'Fundo de Renda Fixa']['CNPJ_FUNDO']   
        cotas_normalizadas = cotas_normalizadas[cnpj_cadastro[cnpj_cadastro.isin(cnpj_informes)]]

    if classe == 'cambial':
        cnpj_cadastro = cadastro[cadastro['CLASSE'] == 'Fundo Cambial']['CNPJ_FUNDO']   
        cotas_normalizadas = cotas_normalizadas[cnpj_cadastro[cnpj_cadastro.isin(cnpj_informes)]]
    
    #melhores
    melhores = pd.DataFrame()
    melhores['retorno(%)'] = round(((cotas_normalizadas.iloc[-1].sort_values(ascending=False)[:top] - 1) * 100),2)
    for cnpj in melhores.index:
        fundo = cadastro[cadastro['CNPJ_FUNDO'] == cnpj]
        melhores.at[cnpj, 'Fundo de Investimento'] = fundo['DENOM_SOCIAL'].values[0]
        melhores.at[cnpj, 'Classe'] = fundo['CLASSE'].values[0]
        melhores.at[cnpj, 'PL'] = fundo['VL_PATRIM_LIQ'].values[0]

    #piores
    piores = pd.DataFrame()
    piores['retorno(%)'] = round(((cotas_normalizadas.iloc[-1].sort_values(ascending=True)[:top] - 1) * 100),2)
    for cnpj in piores.index:
        fundo = cadastro[cadastro['CNPJ_FUNDO'] == cnpj]
        piores.at[cnpj, 'Fundo de Investimento'] = fundo['DENOM_SOCIAL'].values[0]
        piores.at[cnpj, 'Classe'] = fundo['CLASSE'].values[0]
        piores.at[cnpj, 'PL'] = fundo['VL_PATRIM_LIQ'].values[0]
    
    return melhores, piores





if __name__ == '__main__':

    data_atual = datetime.now().strftime("%Y-%m")

    days = datetime.now().strftime("%j")
    data_anterior = (datetime.now() - timedelta(days=(0 + int(days)-int(datetime.now().strftime("%d"))))).strftime("%Y-%m")

    cadastro =  busca_cadastro_cvm()

    informes = busca_informes_diarios_CVM(data_anterior, data_atual)

    melhores , piores = melhores_e_piores(informes, cadastro, top=10, minimo_de_cotistas=100, classe='')


    informes.to_csv('informes.csv', sep=';', encoding='ISO-8859-1')
    informes.to_sql('cvm_informes', con=engine)


    piores.to_csv('piores.csv', sep=';', encoding='ISO-8859-1')
    piores.to_sql('cvm_piores', con=engine)
    

    melhores.to_csv('melhores.csv', sep=';', encoding='ISO-8859-1')
    melhores.to_sql('cvm_melhores', con=engine)
    
    
    
    
    print(melhores.reset_index())
    
