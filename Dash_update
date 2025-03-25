from google.colab import auth
auth.authenticate_user()
import pandas as pd
import numpy as np
import gspread
from google.auth import default

creds, _ = default()
gc = gspread.authorize(creds)

sheet_opdivo = gc.open_by_key('id')
worksheet_opdivo = sheet_opdivo.worksheet('sheet')
data_opdivo = worksheet_opdivo.get_all_values()
opdivo = pd.DataFrame(data_opdivo[1:], columns=data_opdivo[0])


sheet_keytruda = gc.open_by_key('id')
worksheet_keytruda = sheet_keytruda.worksheet('sheet')
data_keytruda = worksheet_keytruda.get_all_values()
keytruda = pd.DataFrame(data_keytruda[1:], columns=data_keytruda[0])
sheet_enhertu = gc.open_by_key('id')
worksheet_enhertu = sheet_enhertu.worksheet('sheet')
data_enhertu = worksheet_enhertu.get_all_values()
enhertu = pd.DataFrame(data_enhertu[1:], columns=data_enhertu[0])

opdivo['Nº da carteirinha'] = opdivo['Nº da carteirinha'].astype(str)
opdivo['Nº da carteirinha'] = opdivo['Nº da carteirinha'].astype(str).str.zfill(17)
opdivo['TIPO_MARCA'] = 'Opdivo'
opdivo.rename(columns={'NME_BENEFICIARIO': 'NME_BENEFICIARIO',
                         'Liminar associada ao medicamento? ': 'LM_Associado_ao_Medicamento',
                         'Subtipo Molecular': 'Subtipo_Molecular',
                         'Idade': 'Idade',
                         'Sexo': 'Sexo',
                         'VPP': 'VPP',

                         'Nº da carteirinha': 'COD_CARTEIRINHA_BENEFICIARIO',
                         'Data da VPP': 'Data_da_VPP','Prestador': 'Prestador',
                         'CID': 'CID','Descrição do CID': 'Descricao_do_CID',
                         'IHQ': 'IHQ','VPP IHQ': 'VPP_IHQ','Estadiamento': 'Estadiamento',
                         'ECOG': 'ECOG','Finalidade do tratamento': 'Finalidade_do_tratamento',
                         'Tipo de Quimioterapia': 'Tipo_de_Quimioterapia',
                         'Plano terapêutico': 'Plano_terapeutico',
                         'Parecer VPP': 'Parecer_VPP',
                         'Relatório Médico X Parecer VPP?': 'Relatorio_Medico_X_Parecer_VPP',
                         'Relatório Médico X Dados estruturados?': 'Relatorio_Medico_X_Dados_estruturados',
                         'Parecer análise GEIES': 'Parecer_analise_GEIES'}, inplace=True)
opdivo = opdivo.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
opdivo['Data_da_VPP'] = pd.to_datetime(opdivo['Data_da_VPP'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
opdivo['VPP'] = opdivo['VPP'].astype(str)
opdivo['COD_CARTEIRINHA_BENEFICIARIO'] = opdivo['COD_CARTEIRINHA_BENEFICIARIO'].astype(str)
opdivo['VPP_IHQ'] = opdivo['VPP_IHQ'].astype(str)
opdivo.replace({'': np.nan, 'null': np.nan}, inplace=True)

keytruda['Nº da carteirinha'] = keytruda['Nº da carteirinha'].astype(str)
keytruda['Nº da carteirinha'] = keytruda['Nº da carteirinha'].astype(str).str.zfill(17)
keytruda['TIPO_MARCA'] = 'keytruda'
keytruda.rename(columns={'NME_BENEFICIARIO': 'NME_BENEFICIARIO',
                         'LM Associado ao Medicamento Keytruda': 'LM_Associado_ao_Medicamento',
                         'Subtipo Molecular': 'Subtipo_Molecular',
                         'Idade': 'Idade',
                         'Sexo': 'Sexo',
                         'VPP': 'VPP',
                         'Nº da carteirinha': 'COD_CARTEIRINHA_BENEFICIARIO',
                         'Data da VPP': 'Data_da_VPP','Prestador': 'Prestador',
                         'CID': 'CID','Descrição CID': 'Descricao_do_CID',
                         'IHQ': 'IHQ','VPP IHQ': 'VPP_IHQ','Estadiamento': 'Estadiamento',
                         'ECOG': 'ECOG','Finalidade do tratamento': 'Finalidade_do_tratamento',
                         'Tipo de Quimioterapia': 'Tipo_de_Quimioterapia',
                         'Plano terapêutico': 'Plano_terapeutico',
                         'Parecer VPP': 'Parecer_VPP',
                         'Relatório Médico X Parecer VPP?': 'Relatorio_Medico_X_Parecer_VPP',
                         'Relatório Médico X Dados estruturados?': 'Relatorio_Medico_X_Dados_estruturados',
                         'Parecer análise GEIES': 'Parecer_analise_GEIES'}, inplace=True)
keytruda = keytruda.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
keytruda['Data_da_VPP'] = pd.to_datetime(keytruda['Data_da_VPP'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
keytruda['VPP'] = keytruda['VPP'].astype(str)
keytruda['COD_CARTEIRINHA_BENEFICIARIO'] = keytruda['COD_CARTEIRINHA_BENEFICIARIO'].astype(str)
keytruda['VPP_IHQ'] = keytruda['VPP_IHQ'].astype(str)
keytruda.replace({'': np.nan, 'null': np.nan}, inplace=True)

enhertu['Nº da carteirinha'] = enhertu['Nº da carteirinha'].astype(str)
enhertu['Nº da carteirinha'] = enhertu['Nº da carteirinha'].astype(str).str.zfill(17)
enhertu['TIPO_MARCA'] = 'enhertu'
enhertu.rename(columns={'NME_BENEFICIARIO': 'NME_BENEFICIARIO',
                        'Liminar associada ao medicamento? ': 'LM_Associado_ao_Medicamento',
                        'Idade': 'Idade',
                        'Subtipo Molecular': 'Subtipo_Molecular',
                          'Sexo': 'Sexo',
                          'VPP': 'VPP',
                          'Nº da carteirinha': 'COD_CARTEIRINHA_BENEFICIARIO',
                          'Data da VPP': 'Data_da_VPP',
                          'Prestador': 'Prestador',
                          'CID': 'CID',
                          'Descrição do CID': 'Descricao_do_CID',
                          'IHQ': 'IHQ',
                          'VPP IHQ': 'VPP_IHQ',
                          'Estadiamento': 'Estadiamento',
                          'ECOG': 'ECOG',
                          'Finalidade do tratamento': 'Finalidade_do_tratamento',
                          'Tipo de Quimioterapia': 'Tipo_de_Quimioterapia',
                          'Plano terapêutico': 'Plano_terapeutico',
                          'Parecer VPP': 'Parecer_VPP',
                          'Relatório Médico X Parecer VPP?': 'Relatorio_Medico_X_Parecer_VPP',
                          'Relatório Médico X Dados estruturados?': 'Relatorio_Medico_X_Dados_estruturados',
                          'Parecer análise GEIES': 'Parecer_analise_GEIES'}, inplace=True)
enhertu = enhertu.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
enhertu['Data_da_VPP'] = pd.to_datetime(enhertu['Data_da_VPP'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
enhertu['VPP'] = enhertu['VPP'].astype(str)
enhertu['COD_CARTEIRINHA_BENEFICIARIO'] = enhertu['COD_CARTEIRINHA_BENEFICIARIO'].astype(str)
enhertu['VPP_IHQ'] = enhertu['VPP_IHQ'].astype(str)
enhertu.replace({'': np.nan, 'null': np.nan}, inplace=True)

df_final = pd.concat([enhertu, keytruda, opdivo], axis=0, join='inner', ignore_index=True)
df_final = df_final.where(pd.notnull(df_final), None)
df = df_final.fillna("")
df['VPP_IHQ'] = df['VPP_IHQ'].astype(str).replace("nan", "")
df['COD_CARTEIRINHA_BENEFICIARIO'] = df['COD_CARTEIRINHA_BENEFICIARIO'].astype(str).replace("nan", "")
df['IHQ'] = df['IHQ'].str.replace('\n', ' ')
df['IHQ'] = df['IHQ'].str.replace('\r', ' ')
df['VPP_IHQ'] = df['VPP_IHQ'].str.replace('\n', ' ')
df['VPP_IHQ'] = df['VPP_IHQ'].str.replace('\r', ' ')
df['Parecer_VPP'] = df['Parecer_VPP'].str.replace('\n', ' ')
df['Parecer_VPP'] = df['Parecer_VPP'].str.replace('\r', ' ')
df['Parecer_VPP'] = df['Parecer_VPP'].str.replace(';', ',')
df['Plano_terapeutico'] = df['Plano_terapeutico'].str.replace('\n', ' ')
df['Plano_terapeutico'] = df['Plano_terapeutico'].str.replace('\r', ' ')
df['Sexo'] = np.where(df['Sexo'] == 'F', 'Feminino', df['Sexo'])
df['Sexo'] = np.where(df['Sexo'] == 'M', 'Masculino', df['Sexo'])
df['Observações'] = df['Observações'].str.replace('\n', ' ')
df['Observações'] = df['Observações'].str.replace('\r', ' ')
date_columns = ["Data_da_VPP"]
for col in date_columns: df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
df = df.applymap(lambda x: str(x).replace("\t", " ") if isinstance(x, str) else x)
df = df.applymap(lambda x: x.replace('"', '') if isinstance(x, str) else x)
df = df.drop('Observações', axis=1)
#df = df.drop('Subtipo Molecular', axis=1)
df = df.astype(str)

from google.cloud import bigquery
auth.authenticate_user()
project_id = 'project'
dataset_id = 'dataset'
table_id = 'table'
client = bigquery.Client(project=project_id)
table_ref = client.dataset(dataset_id).table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
job = client.load_table_from_dataframe(
    df,
    table_ref,
    job_config=job_config
)
job.result()
print(f"DataFrame carregado para {project_id}.{dataset_id}.{table_id}")

with open('/content/drive/Shareddrives/GEIES/2 - Programas | Projetos Estratégicos/2.4 - Medicamentos/Dash top 10 medicamentos/Auto_dash_top10_medicamentos.sql', 'r') as f:
  sql_query = f.read()

auth.authenticate_user()
project_id = 'project'
dataset_id = 'dataset'
table_id = 'table'

client = bigquery.Client(project=project_id)


job_config = bigquery.QueryJobConfig(
    destination=f'{project_id}.{dataset_id}.{table_id}',
    write_disposition='WRITE_TRUNCATE'
)


query_job = client.query(sql_query, job_config=job_config)
query_job.result()

print(f"Query executada e resultados salvos em {project_id}.{dataset_id}.{table_id}")
