import json
import re

import pandas as pd
import pyodbc
import requests
from flask import Flask, Response, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

with open('mudar_base.txt', 'r') as arquivo:
    # Leia o conteúdo do arquivo
    conteudo = arquivo.read()


if 'PROD' in conteudo:
    login_url = 'https://apisafe.siqcarrefour.com.br/login'
    send_order_url = 'https://apisafe.siqcarrefour.com.br/send-request/traceability/send-order'
    server = 'SPL-MSQLCLU-04\protheus'
    database = 'PROTHEUS'
    username = 'python.svc'
    password = '6Z*R*j:M1y6QplQ0'
else:
    login_url = 'http://qa.agrotransparencia.com.br:9000/login'
    send_order_url = 'http://qa.agrotransparencia.com.br:9000/send-request/traceability/send-order'
    server = 'SPL-SQLDEV-01\SQLPROTHEUS'
    database = 'PROTHEUS_DEV'
    username = 'python.svc'
    password = '6Z*R*j:M1y6QplQ0'

def zkt_log(process,data,status, sif, numero_pedido, retorno):

    if sif == '4060':
        sif = '0102'
    elif sif == '2484':
        sif = '0103'
    elif sif == '3920':
        sif = '0104'
    elif sif == '3215':
        sif = '0105'

    json_string = json.dumps(data)
    json_bytes = json_string.encode('utf-8')
    hex_representation = json_bytes.hex()

    json_string = json.dumps(retorno)
    json_bytes = json_string.encode('utf-8')
    hex_representation_retorno = json_bytes.hex()


    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    cursor.execute("SELECT MAX(R_E_C_N_O_) FROM ZKT010")
    results = cursor.fetchall()
    text = str(results)
    regex_syntax = r"\D"
    num_str = re.sub(regex_syntax, "", text)
    num = int(num_str)
    numId = num + 1
     # Lembre-se de definir o valor de 'status' corretamente

    insert_query = f"""
        INSERT INTO ZKT010(ZKT_FILORI, ZKT_ONERGY,ZKT_ROTINA,ZKT_ID, ZKT_JSON, ZKT_RETURN,ZKT_DTIMP, ZKT_HRIMP, ZKT_STATUS, R_E_C_N_O_)
        VALUES (
            '{sif}',
            '{numero_pedido}',
            'SAFE_TR',
            '{process}',
            0x{hex_representation},
            0X{hex_representation_retorno},
            CONVERT(VARCHAR(8), GETDATE(), 112),
            CONVERT(VARCHAR(5), GETDATE(), 108),
            '{status}',
            '{numId}'
        );
    """

    # print(insert_query)

    cursor.execute(insert_query)
    cursor.commit()

    cursor.close()
    cnxn.close()


@app.route("/", methods=["GET"])
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/abate", methods=["POST"])
def abate():
    file = request.files["file"]

    df = pd.read_excel(file, names=['abate','numero_pedido','frigorifico_email_retorno','frigorifico_razao_social','frigorifico_documento','frigorifico_sif','frigorifico_endereco_uf','frigorifico_endereco_municipio','processos_abates_fazenda_codigo','processos_abates_fazenda_documento','procecssos_abates_fazenda_nome_fantasia','processos_abates_fazenda_razao_social','processos_abates_fazenda_endereco_uf','processos_abates_fazenda_endereco_municipio','processos_abates_fazenda_latitude','processos_abates_fazenda_longitude','processos_abates_fazenda_nirf','processos_abates_fazenda_inscricao_estadual','processos_abates_fazenda_inscricao_car','processos_abates_notas_fiscais','processos_abates_notas_fiscais_serie','processos_abates_notas_fiscais_chave','processos_abates_gta_numero','processos_abates_gta_serie','processos_abates_gta_quantidade','processos_abates_itens_data_abate','processos_abates_itens_lote_abate','processos_abates_itens_numero_brinco','processos_abates_itens_sequencia_abate_lote','processos_abates_itens_sexo','processos_abates_itens_dentes','processos_abates_itens_acabamento','processos_abates_itens_peso','processos_abates_itens_peso_banda_A','processos_abates_itens_peso_banda_B','processos_abates_itens_data_desossa','processos_abates_itens_lote_desossa','processos_abates_itens_sif_desossa'])
    df.fillna('', inplace=True)
    # print(df.head)
    pedidos_dict = {}


    for index, row in df.iterrows():
        if index != 0:
            numero_pedido = row['numero_pedido']
            frigorifico_email_retorno = str(row['frigorifico_email_retorno']).strip()
            frigorifico_razao_social = str(row['frigorifico_razao_social']).strip()
            frigorifico_documento = str(row['frigorifico_documento']).strip()
            if frigorifico_documento == '':
                frigorifico_documento = '0'
            frigorifico_sif = str(row['frigorifico_sif']).strip()
            frigorifico_endereco_uf = str(row['frigorifico_endereco_uf']).strip()
            frigorifico_endereco_municipio = str(row['frigorifico_endereco_municipio']).strip()
            processos_abates_fazenda_codigo = str(row['processos_abates_fazenda_codigo']).strip()
            if processos_abates_fazenda_codigo == '':
                processos_abates_fazenda_codigo = '0'
            processos_abates_fazenda_documento = str(row['processos_abates_fazenda_documento']).strip() 
            if processos_abates_fazenda_documento == '':
                processos_abates_fazenda_documento = '0'
            procecssos_abates_fazenda_nome_fantasia = str(row['procecssos_abates_fazenda_nome_fantasia']).strip() 
            processos_abates_fazenda_razao_social = str(row['processos_abates_fazenda_razao_social']).strip() 
            processos_abates_fazenda_endereco_uf = str(row['processos_abates_fazenda_endereco_uf']).strip() 
            processos_abates_fazenda_endereco_municipio = str(row['processos_abates_fazenda_endereco_municipio']).strip() 
            processos_abates_fazenda_latitude = str(row['processos_abates_fazenda_latitude']).strip() 
            processos_abates_fazenda_longitude = str(row['processos_abates_fazenda_longitude']).strip() 
            processos_abates_fazenda_nirf = str(row['processos_abates_fazenda_nirf']).strip() 
            processos_abates_fazenda_inscricao_estadual = str(row['processos_abates_fazenda_inscricao_estadual']).strip() 
            processos_abates_fazenda_inscricao_car = str(row['processos_abates_fazenda_inscricao_car']).strip() 
            processos_abates_notas_fiscais = str(row['processos_abates_notas_fiscais']).strip() 
            processos_abates_notas_fiscais_serie = str(row['processos_abates_notas_fiscais_serie']).strip() 
            processos_abates_notas_fiscais_chave = str(row['processos_abates_notas_fiscais_chave']).strip() 
            processos_abates_gta_numero = str(row['processos_abates_gta_numero']).strip() 
            processos_abates_gta_serie = str(row['processos_abates_gta_serie']).strip() 
            processos_abates_gta_quantidade = str(row['processos_abates_gta_quantidade']).strip() 
            if processos_abates_gta_quantidade == '':
                processos_abates_gta_quantidade = '0'
            processos_abates_itens_data_abate = str(row['processos_abates_itens_data_abate']).strip() 
            processos_abates_itens_lote_abate = str(row['processos_abates_itens_lote_abate']).strip() 
            processos_abates_itens_numero_brinco = str(row['processos_abates_itens_numero_brinco']).strip() 
            processos_abates_itens_sequencia_abate_lote = str(row['processos_abates_itens_sequencia_abate_lote']).strip() 
            processos_abates_itens_sexo = str(row['processos_abates_itens_sexo']).strip() 
            processos_abates_itens_dentes = str(row['processos_abates_itens_dentes']).strip() 
            processos_abates_itens_acabamento = str(row['processos_abates_itens_acabamento']).strip() 
            processos_abates_itens_peso = str(row['processos_abates_itens_peso']).strip() 
            processos_abates_itens_peso_banda_A = str(row['processos_abates_itens_peso_banda_A']).strip() 
            if processos_abates_itens_peso_banda_A == '':
                processos_abates_itens_peso_banda_A = '0'
            processos_abates_itens_peso_banda_B = str(row['processos_abates_itens_peso_banda_B']).strip() 
            if processos_abates_itens_peso_banda_B == '':
                processos_abates_itens_peso_banda_B = '0'
            processos_abates_itens_data_desossa = str(row['processos_abates_itens_data_desossa']).strip() 
            processos_abates_itens_lote_desossa = str(row['processos_abates_itens_lote_desossa']).strip() 
            processos_abates_itens_sif_desossa = str(row['processos_abates_itens_sif_desossa']).strip() 

            abate_data = {
                "fazenda": {
                    "codigo": processos_abates_fazenda_codigo,
                    "documento": processos_abates_fazenda_documento,
                    "nome_fantasia": procecssos_abates_fazenda_nome_fantasia,
                    "razao_social": processos_abates_fazenda_razao_social,
                    "endereco": {
                        "uf": processos_abates_fazenda_endereco_uf,
                        "municipio": processos_abates_fazenda_endereco_municipio
                    },
                    "latitude": processos_abates_fazenda_latitude,
                    "longitude": processos_abates_fazenda_longitude,
                    "nirf": processos_abates_fazenda_nirf,
                    "inscricao_estadual": processos_abates_fazenda_inscricao_estadual,
                    "inscricao_car": [processos_abates_fazenda_inscricao_car]
                },
                "notasFiscais": [
                    {
                        "numero": processos_abates_notas_fiscais,
                        "serie": processos_abates_notas_fiscais_serie,
                        "chave": processos_abates_notas_fiscais_chave
                    }
                ],
                "gta": [
                    {
                        "numero": processos_abates_gta_numero,
                        "serie": processos_abates_gta_serie,
                        "quantidade": processos_abates_gta_quantidade
                    }
                ],
                "itens": [
                    {
                        "dataAbate": processos_abates_itens_data_abate,
                        "loteAbate": processos_abates_itens_lote_abate,
                        "numeroBrinco": processos_abates_itens_numero_brinco,
                        "sequenciaAbateLote": processos_abates_itens_sequencia_abate_lote,
                        "sexo": processos_abates_itens_sexo,
                        "dentes": processos_abates_itens_dentes,
                        "acabamento": processos_abates_itens_acabamento,
                        "peso": {
                            "bandaA": processos_abates_itens_peso_banda_A,
                            "bandaB": processos_abates_itens_peso_banda_B
                        },
                        "dataDesossa": processos_abates_itens_data_desossa,
                        "loteDesossa": processos_abates_itens_lote_desossa,
                        "sifDesossa": processos_abates_itens_sif_desossa
                    }
                ]
            }

            if numero_pedido in pedidos_dict:
                notas_fiscais_set = set()
                gta_set = set()
                itens_set = set()

                abates = pedidos_dict[numero_pedido]["processos"]["abates"]

                for abate in abates:
                    for nf in abate["notasFiscais"]:
                        nf_tuple = (nf["numero"], nf["serie"], nf["chave"])
                        notas_fiscais_set.add(nf_tuple)

                    for gta in abate["gta"]:
                        gta_tuple = (gta["numero"], gta["serie"], gta["quantidade"])
                        gta_set.add(gta_tuple)

                    for item in abate["itens"]:
                        item_tuple = (
                            item["dataAbate"],
                            item["loteAbate"],
                            item["numeroBrinco"],
                            item["sequenciaAbateLote"],
                            item["sexo"],
                            item["dentes"],
                            item["acabamento"],
                            item["peso"]["bandaA"],
                            item["peso"]["bandaB"],
                            item["dataDesossa"],
                            item["loteDesossa"],
                            item["sifDesossa"],
                        )
                        itens_set.add(item_tuple)

                nf_tuple = (processos_abates_notas_fiscais, processos_abates_notas_fiscais_serie, processos_abates_notas_fiscais_chave)
                gta_tuple = (processos_abates_gta_numero, processos_abates_gta_serie, processos_abates_gta_quantidade)
                item_tuple = (
                    processos_abates_itens_data_abate,
                    processos_abates_itens_lote_abate,
                    processos_abates_itens_numero_brinco,
                    processos_abates_itens_sequencia_abate_lote,
                    processos_abates_itens_sexo,
                    processos_abates_itens_dentes,
                    processos_abates_itens_acabamento,
                    processos_abates_itens_peso_banda_A,
                    processos_abates_itens_peso_banda_B,
                    processos_abates_itens_data_desossa,
                    processos_abates_itens_lote_desossa,
                    processos_abates_itens_sif_desossa,
                )

                if nf_tuple not in notas_fiscais_set:
                    pedidos_dict[numero_pedido]["processos"]["abates"][0]["notasFiscais"].append({
                        "numero": processos_abates_notas_fiscais,
                        "serie": processos_abates_notas_fiscais_serie,
                        "chave": processos_abates_notas_fiscais_chave
                    })

                if gta_tuple not in gta_set:
                    pedidos_dict[numero_pedido]["processos"]["abates"][0]["gta"].append({
                        "numero": processos_abates_gta_numero,
                        "serie": processos_abates_gta_serie,
                        "quantidade": processos_abates_gta_quantidade
                    })

                if item_tuple not in itens_set:
                    pedidos_dict[numero_pedido]["processos"]["abates"][0]["itens"].append({
                        "dataAbate": processos_abates_itens_data_abate,
                        "loteAbate": processos_abates_itens_lote_abate,
                        "numeroBrinco": processos_abates_itens_numero_brinco,
                        "sequenciaAbateLote": processos_abates_itens_sequencia_abate_lote,
                        "sexo": processos_abates_itens_sexo,
                        "dentes": processos_abates_itens_dentes,
                        "acabamento": processos_abates_itens_acabamento,
                        "peso": {
                            "bandaA": processos_abates_itens_peso_banda_A,
                            "bandaB": processos_abates_itens_peso_banda_B
                        },
                        "dataDesossa": processos_abates_itens_data_desossa,
                        "loteDesossa": processos_abates_itens_lote_desossa,
                        "sifDesossa": processos_abates_itens_sif_desossa
                    })

            else:
                pedido_data = {
                    "numeroPedido": numero_pedido,
                    "frigorifico": {
                        "emailRetorno": frigorifico_email_retorno,
                        "razao_social": frigorifico_razao_social,
                        "documento": frigorifico_documento,
                        "sif": frigorifico_sif,
                        "endereco": {
                            "uf": frigorifico_endereco_uf,
                            "municipio": frigorifico_endereco_municipio
                        }
                    },
                    "processos": {
                        "abates": [abate_data]
                            }
                            }


                pedidos_dict[numero_pedido] = pedido_data

    pedidos_lista = list(pedidos_dict.values())
    # print(pedidos_lista)
    obj_abate = pedidos_lista

    post_login = requests.post(login_url, json={'username': '01701', 'password': 'G8mb0dijUj0'}, verify=False)

    if post_login.status_code == 200:

        token = post_login.text

        # print(token)

        head = {'Authorization' : token}

        pedidos_err = []
        pedidos_sucess = []

        for pedido in obj_abate:
                send_order = requests.post(send_order_url,json=pedido,headers=head, verify=False)
                json_pedido = send_order.json()
                status = json_pedido['status']
                # print(status)
                if status == 'success':
                    zkt_log('ABATE',pedido,2, frigorifico_sif, pedido['numeroPedido'], json_pedido)

                    pedidos_sucess.append(pedido['numeroPedido'])
                    
                else:
                    zkt_log('ABATE',pedido,3, frigorifico_sif, pedido['numeroPedido'], json_pedido)

                    
                    pedidos_err.append(pedido['numeroPedido'])

        if(len(pedidos_err)>=1):

            return{"Erros":pedidos_err},400
        
        else:

            return {"Enviados":pedidos_sucess},200

    else:
        return {"msg":"login inválido"},401
        

@app.route("/desossa", methods=["POST"])
def desossa():
    file = request.files["file"]
    df = pd.read_excel(file, names=['desossa','numero_pedido','frigorifico_email_retorno','frigorifico_razao_social','frigorifico_documento','frigorifico_sif','frigorifico_endereco_uf','frigorifico_endereco_municipio','processos_desossa_frigorifico_email_retorno','processos_desossa_frigorifico_razao_social','processos_desossa_frigorifico_documento','processos_desossa_frigorifico_sif','processos_desossa_frigorifico_endereco','processos_desossa_frigorifico_uf','processos_desossa_frigorifico_municipio','processos_desossa_numero_pedido','processos_desossa_data_desossa','processos_desossa_lotes_desossa_inter','processos_desossa_lotes_desossa_lote_mp','processos_desossa_lotes_desossa_documento','processos_desossa_lotes_desossa_nota_fiscal_origem_numero','processos_desossa_lotes_desossa_nota_fiscal_origem_serie','processos_desossa_lotes_desossa_nota_fiscal_origem_chave','processos_desossa_produtos_codigo_ean','processos_desossa_produtos_nome','processos_desossa_produtos_data_producao','processos_desossa_produtos_data_validade','processos_desossa_produtos_peso_liquido','processos_desossa_produtos_id_caixa','processos_desossa_produtos_id_palete','processos_desossa_produtos_nota_fiscal_numero','processos_desossa_produtos_nota_fiscal_serie','processos_desossa_produtos_nota_fiscal_chave','processos_desossa_produtos_documento_destino'])
    df.fillna('', inplace=True)
    pedidos_dict = {}

    for index, row in df.iterrows():
        if index != 0:
            numero_pedido = row['numero_pedido']
            frigorifico_email_retorno = str(row['frigorifico_email_retorno']).strip() 
            frigorifico_razao_social = str(row['frigorifico_razao_social']).strip() 
            frigorifico_documento = str(row['frigorifico_documento']).strip() 
            frigorifico_sif = str(row['frigorifico_sif']).strip() 
            frigorifico_endereco_uf = str(row['frigorifico_endereco_uf']).strip() 
            frigorifico_endereco_municipio = str(row['frigorifico_endereco_municipio']).strip() 
            processos_desossa_frigorifico_email_retorno = str(row['processos_desossa_frigorifico_email_retorno']).strip() 
            processos_desossa_frigorifico_razao_social = str(row['processos_desossa_frigorifico_razao_social']).strip() 
            processos_desossa_frigorifico_documento = str(row['processos_desossa_frigorifico_documento']).strip() 
            processos_desossa_frigorifico_sif = str(row['processos_desossa_frigorifico_sif']).strip() 
            processos_desossa_frigorifico_endereco = str(row['processos_desossa_frigorifico_endereco']).strip() 
            processos_desossa_frigorifico_uf = str(row['processos_desossa_frigorifico_uf']).strip() 
            processos_desossa_frigorifico_municipio = str(row['processos_desossa_frigorifico_municipio']).strip() 
            processos_desossa_numero_pedido = str(row['processos_desossa_numero_pedido']).strip()
            if processos_desossa_numero_pedido == '':
                processos_desossa_numero_pedido = '0'
            processos_desossa_data_desossa = str(row['processos_desossa_data_desossa']).strip() 
            processos_desossa_lotes_desossa_inter = str(row['processos_desossa_lotes_desossa_inter']).strip()
            if processos_desossa_lotes_desossa_inter == '':
                processos_desossa_lotes_desossa_inter = '1900-01-01'
            processos_desossa_lotes_desossa_lote_mp = str(row['processos_desossa_lotes_desossa_lote_mp']).strip()
            if processos_desossa_lotes_desossa_lote_mp == '':
                processos_desossa_lotes_desossa_lote_mp = '1900-01-01'
            processos_desossa_lotes_desossa_documento = str(row['processos_desossa_lotes_desossa_documento']).strip() 
            if processos_desossa_lotes_desossa_documento == '':
                processos_desossa_lotes_desossa_documento = '0'
            processos_desossa_lotes_desossa_nota_fiscal_origem_numero = str(row['processos_desossa_lotes_desossa_nota_fiscal_origem_numero']).strip() 
            # if processos_desossa_lotes_desossa_nota_fiscal_origem_numero == '':
            #     processos_desossa_lotes_desossa_nota_fiscal_origem_numero = '00'
            processos_desossa_lotes_desossa_nota_fiscal_origem_serie = str(row['processos_desossa_lotes_desossa_nota_fiscal_origem_serie']).strip()
            processos_desossa_lotes_desossa_nota_fiscal_origem_chave = str(row['processos_desossa_lotes_desossa_nota_fiscal_origem_chave']).strip() 
            processos_desossa_produtos_codigo_ean = str(row['processos_desossa_produtos_codigo_ean']).strip() 
            processos_desossa_produtos_nome = str(row['processos_desossa_produtos_nome']).strip() 
            processos_desossa_produtos_data_producao = str(row['processos_desossa_produtos_data_producao']).strip() 
            processos_desossa_produtos_data_validade = str(row['processos_desossa_produtos_data_validade']).strip() 
            processos_desossa_produtos_peso_liquido = str(row['processos_desossa_produtos_peso_liquido']).strip() 
            if processos_desossa_produtos_peso_liquido == '':
                processos_desossa_produtos_peso_liquido = '0'
            processos_desossa_produtos_id_caixa = str(row['processos_desossa_produtos_id_caixa']).strip() 
            processos_desossa_produtos_id_palete = str(row['processos_desossa_produtos_id_palete']).strip() 
            processos_desossa_produtos_nota_fiscal_numero = str(row['processos_desossa_produtos_nota_fiscal_numero']).strip() 
            processos_desossa_produtos_nota_fiscal_serie = str(row['processos_desossa_produtos_nota_fiscal_serie']).strip() 
            processos_desossa_produtos_nota_fiscal_chave = str(row['processos_desossa_produtos_nota_fiscal_chave']).strip()
            processos_desossa_produtos_documento_destino = str(row['processos_desossa_produtos_documento_destino']).strip()
            if processos_desossa_produtos_documento_destino == '':
                processos_desossa_produtos_documento_destino = '0'
            desossa_data = {
                "frigorifico": {
                    "emailRetorno": processos_desossa_frigorifico_email_retorno,
                    "razao_social": processos_desossa_frigorifico_razao_social,
                    "documento": processos_desossa_frigorifico_documento,
                    "sif": processos_desossa_frigorifico_sif,
                    "endereco": {
                        "uf": processos_desossa_frigorifico_uf,
                        "municipio": processos_desossa_frigorifico_municipio
                    }
                },
                "numeroPedido": processos_desossa_numero_pedido,
                "dataDesossa": processos_desossa_data_desossa,
                "lotesDesossa": [
                    {
                        "inter": processos_desossa_lotes_desossa_inter,
                        "loteMP": processos_desossa_lotes_desossa_lote_mp,
                        "documento": processos_desossa_lotes_desossa_documento,
                        "notaFiscalOrigem": {
                            "numero": processos_desossa_lotes_desossa_nota_fiscal_origem_numero,
                            "serie": processos_desossa_lotes_desossa_nota_fiscal_origem_serie,
                            "chave": processos_desossa_lotes_desossa_nota_fiscal_origem_chave
                        }
                    }
                ],
                "produtos": [
                    {
                        "codigo_ean": processos_desossa_produtos_codigo_ean,
                        "nome": processos_desossa_produtos_nome,
                        "dataProducao": processos_desossa_produtos_data_producao,
                        "dataValidade": processos_desossa_produtos_data_validade,
                        "pesoLiquido": float(processos_desossa_produtos_peso_liquido),
                        "idCaixa": processos_desossa_produtos_id_caixa,
                        "idPalete": processos_desossa_produtos_id_palete,
                        "notaFiscal": {
                            "numero": processos_desossa_produtos_nota_fiscal_numero,
                            "serie": processos_desossa_produtos_nota_fiscal_serie,
                            "chave": processos_desossa_produtos_nota_fiscal_chave
                        },
                        "documentoDestino": processos_desossa_produtos_documento_destino
                    }
                ]
            }

            if numero_pedido in pedidos_dict:

                
                pedidos_dict[numero_pedido]["processos"]["desossa"]["lotesDesossa"].append({
                    "inter": processos_desossa_lotes_desossa_inter,
                    "loteMP": processos_desossa_lotes_desossa_lote_mp,
                    "documento": processos_desossa_lotes_desossa_documento,
                    "notaFiscalOrigem": {
                        "numero": processos_desossa_lotes_desossa_nota_fiscal_origem_numero,
                        "serie": processos_desossa_lotes_desossa_nota_fiscal_origem_serie,
                        "chave": processos_desossa_lotes_desossa_nota_fiscal_origem_chave
                    }
                })

                pedidos_dict[numero_pedido]["processos"]["desossa"]["produtos"].append({

                        "codigo_ean": processos_desossa_produtos_codigo_ean,
                        "nome": processos_desossa_produtos_nome,
                        "dataProducao": processos_desossa_produtos_data_producao,
                        "dataValidade": processos_desossa_produtos_data_validade,
                        "pesoLiquido": float(processos_desossa_produtos_peso_liquido),
                        "idCaixa": processos_desossa_produtos_id_caixa,
                        "idPalete": processos_desossa_produtos_id_palete,
                        "notaFiscal": {
                            "numero": processos_desossa_produtos_nota_fiscal_numero,
                            "serie": processos_desossa_produtos_nota_fiscal_serie,
                            "chave": processos_desossa_produtos_nota_fiscal_chave
                        },
                        "documentoDestino": processos_desossa_produtos_documento_destino

                })

            else:
                pedido_data = {
                    "numeroPedido": numero_pedido,
                    "frigorifico": {
                        "emailRetorno": frigorifico_email_retorno,
                        "razao_social": frigorifico_razao_social,
                        "documento": frigorifico_documento,
                        "sif": frigorifico_sif,
                        "endereco": {
                            "uf": frigorifico_endereco_uf,
                            "municipio": frigorifico_endereco_municipio
                        }
                    },
                    "processos": {
                        "abates": [],
                        "desossa": desossa_data
                    }
                }

                pedidos_dict[numero_pedido] = pedido_data

    pedidos_lista = list(pedidos_dict.values())
    obj_desossa = pedidos_lista

    post_login = requests.post(login_url, json={'username': '01701', 'password': 'G8mb0dijUj0'}, verify=False)

    if post_login.status_code == 200:

        token = post_login.text
        # print(token)

        cabecalho = {'Authorization' : token}

        pedidos_err = []
        pedidos_sucess = []

        for pedido in obj_desossa:


           
                enviar_pedido = requests.post(send_order_url, json=pedido, headers=cabecalho, verify=False)
                json_pedido = enviar_pedido.json()
                status = json_pedido['status']
                # print(status)
                if status == 'success':
                    zkt_log('DESOS',pedido,2, frigorifico_sif, pedido['numeroPedido'], json_pedido)

                    pedidos_sucess.append(pedido['numeroPedido'])

                   
                else:
                    zkt_log('DESOS',pedido,3, frigorifico_sif, pedido['numeroPedido'], json_pedido)

                    pedidos_err.append(pedido['numeroPedido'])

                   
                
                
        if(len(pedidos_err)>=1):

            return{"Erros":pedidos_err},400
        
        else:

            return {"Enviados":pedidos_sucess},200
            


    else:

        return {"msg":"login inválido"},401
    


