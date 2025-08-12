import os
from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from hdbcli import dbapi
from dotenv import load_dotenv
import pandas as pd
import db as db
import requests
import json
from oauth_client import OAuthClient

# Carrega variáveis do .env
load_dotenv() 

DSP_token_url = os.getenv("DSP_token_url")
DSP_client_id = os.getenv("DSP_client_id")
DSP_client_secret = os.getenv("DSP_client_secret")

# Inicia o app Flask
app = Flask(__name__)
app.secret_key = 'your_secret_key' 

# Função para conectar ao HANA Cloud

# Rota principal
@app.route('/')
def index():
    result = db.query_table_names()
    return render_template('index.html', tabelas=result)

@app.route('/tabelas/<table>')
def tabelas(table):
    tabela = db.query_table_data(table)
    return render_template("tabelas/tabelas.html", tabelas=tabela)

@app.route('/T_BPC_Rates')
def T_BPC_Rates():
    tabela = db.query_T_BPC_Rates()
    return render_template("tabelas/tabelas.html", tabelas=tabela)


@app.route('/download')
def download():
    return render_template("download/download.html")

@app.route('/menu')
def menu():
    return render_template('menu.html')

@app.route('/showtoken')
def showtoken():
    access_token =  session['access_token']
    return access_token


@app.route('/odata/GV_REPLICON_UNION')
def GV_REPLICON_UNION():
    session['access_token'] = request.args.get("access_token")    
    token =  request.args.get("access_token")    
    client = OAuthClient(
         token_url=DSP_token_url,
         client_id=DSP_client_id,
         client_secret=DSP_client_secret,
         code=None
     )
    
    odata_url="https://crl-np-dsp.us10.hcs.cloud.sap/api/v1/dwc/consumption/relational/INITIAL/GV_REPLICON_UNION/GV_REPLICON_UNION?$top=10"
    try:
        result = client.call_api_session(odata_url)
        print("✅ API result:")
        for row in result.get("value", []):
            print(row)
    except Exception as e:
        print("❌ Error:", e)    

    records = result.get("value", [])
    
    return result

@app.route('/model')
def model():
    odata_url = request.args.get('odata_url')

    client = OAuthClient(
         token_url=DSP_token_url,
         client_id=DSP_client_id,
         client_secret=DSP_client_secret,
         code=None
     )
    
  #  odata_url="https://crl-np-dsp.us10.hcs.cloud.sap/api/v1/dwc/consumption/relational/INITIAL/GV_REPLICON_UNION/GV_REPLICON_UNION?$top=10"
    try:
        result = client.call_api_session(odata_url)
        print("✅ API result:")
        for row in result.get("value", []):
            print(row)
    except Exception as e:
        print("❌ Error:", e)    

    #tabela = [tuple(d.values()) for d in result]
    #print('Tabela', tabela)

    # 📊 Load to DataFrame
    records = result.get("value", [])

    df = pd.DataFrame(records)

    # ✅ Preview
    print(df.head())

    # 📁 Export
    df.to_csv("sap_datasphere_data.csv", index=False)
    df.to_excel("sap_datasphere_data.xlsx", index=False)

    print("✅ Data saved to CSV and Excel.")
    #return redirect('/download')
    tabela = []
    names = df.columns.tolist()
    values = list(df.itertuples(index=False, name=None))
    tabela.append({ 
                "table_name" :  odata_url,          
                "column_names": names,
                "column_values": values
            })

    return render_template("tabelas/tabelas.html", tabelas=tabela)


@app.route('/returntoken')
def returntoken():
   code = request.args.get("code")
   print(code)
   client = OAuthClient(
        token_url=DSP_token_url,
        client_id=DSP_client_id,
        client_secret=DSP_client_secret,
        code=code
    )
   session['access_token'] = client.get_access_token();
   return jsonify({"result" : client.get_access_token()});



@app.route('/fixedtoken',  methods=['GET', 'POST'])
def fixedtoken():
    return jsonify({"access_token":"eyJhbGciOiJSUzI1NiIsImprdSI6Imh0dHBzOi8vY3JsLW5wLWRzcC5hdXRoZW50aWNhdGlvbi51czEwLmhhbmEub25kZW1hbmQuY29tL3Rva2VuX2tleXMiLCJraWQiOiJkZWZhdWx0LWp3dC1rZXktYTdlMGQzOTRhMiIsInR5cCI6IkpXVCIsImppZCI6ICJBU1NqNy9BQ1V3OFNISHVyMk5UYlI3QUJQVkM5emVncjFXdTF4K0ZIYXhzPSJ9.eyJqdGkiOiI4OGVjZGY0NDkyODk0MDA2YjEwNDljMWY3ZGExOWM0MSIsImV4dF9hdHRyIjp7ImVuaGFuY2VyIjoiWFNVQUEiLCJzdWJhY2NvdW50aWQiOiJiZGM3NDQ0Yi04OTI2LTRkYmUtYjVlYS02OGNiMzkwOGM1MzkiLCJ6ZG4iOiJjcmwtbnAtZHNwIiwic2VydmljZWluc3RhbmNlaWQiOiI4ZDU0ZmJjMi03Njg5LTRmYzUtODlhNS03NGVmY2NkNWI0M2IifSwieHMuc3lzdGVtLmF0dHJpYnV0ZXMiOnsieHMuc2FtbC5ncm91cHMiOlsic2FjIl0sInhzLnJvbGVjb2xsZWN0aW9ucyI6WyJzYWMudXNlcnMiXX0sImdpdmVuX25hbWUiOiJjcjI1MDQ2NyIsInhzLnVzZXIuYXR0cmlidXRlcyI6e30sImZhbWlseV9uYW1lIjoiY2hhcmxlc3JpdmVybGFicy5jb20iLCJzdWIiOiI1NzE3MDQzMy00NDNlLTRjMWUtYTZmYy1mY2IxMzRlMzZlYTEiLCJzY29wZSI6WyJvcGVuaWQiLCJhcHByb3V0ZXItc2FjLXNhY3VzMTAhdDY1NS5zYXAuZnBhLnVzZXIiLCJ1YWEudXNlciJdLCJjbGllbnRfaWQiOiJzYi02MGE0MTczYy0yZmExLTRhMGYtYWQxNC01Y2QyMjVjOWNjMDMhYjMzNzQyOHxjbGllbnQhYjY1NSIsImNpZCI6InNiLTYwYTQxNzNjLTJmYTEtNGEwZi1hZDE0LTVjZDIyNWM5Y2MwMyFiMzM3NDI4fGNsaWVudCFiNjU1IiwiYXpwIjoic2ItNjBhNDE3M2MtMmZhMS00YTBmLWFkMTQtNWNkMjI1YzljYzAzIWIzMzc0Mjh8Y2xpZW50IWI2NTUiLCJncmFudF90eXBlIjoiYXV0aG9yaXphdGlvbl9jb2RlIiwidXNlcl9pZCI6IjU3MTcwNDMzLTQ0M2UtNGMxZS1hNmZjLWZjYjEzNGUzNmVhMSIsIm9yaWdpbiI6ImhhbmFjbG91ZHNlcnZpY2VzLXVzLmFjY291bnRzLm9uZGVtYSIsInVzZXJfbmFtZSI6ImNyMjUwNDY3QGNoYXJsZXNyaXZlcmxhYnMuY29tIiwiZW1haWwiOiJjcjI1MDQ2N0BjaGFybGVzcml2ZXJsYWJzLmNvbSIsImF1dGhfdGltZSI6MTc1NTAwNjc1MywicmV2X3NpZyI6ImE4OTdhNGQwIiwiaWF0IjoxNzU1MDA2NzY0LCJleHAiOjE3NTUwMTAzNjQsImlzcyI6Imh0dHBzOi8vY3JsLW5wLWRzcC5hdXRoZW50aWNhdGlvbi51czEwLmhhbmEub25kZW1hbmQuY29tL29hdXRoL3Rva2VuIiwiemlkIjoiYmRjNzQ0NGItODkyNi00ZGJlLWI1ZWEtNjhjYjM5MDhjNTM5IiwiYXVkIjpbInVhYSIsIm9wZW5pZCIsInNiLTYwYTQxNzNjLTJmYTEtNGEwZi1hZDE0LTVjZDIyNWM5Y2MwMyFiMzM3NDI4fGNsaWVudCFiNjU1IiwiYXBwcm91dGVyLXNhYy1zYWN1czEwIXQ2NTUuc2FwLmZwYSJdfQ.akW3voTaABHL3nOg2mpSXtnMZOmlBOsCjWnt7Lo0DkoX10jprICsGkDkneER16IBERqrE7mlsozoAmJ9oZ8WfVyowufkAcoV9L0oWsgvzu08b187QRuQZr_KS1ElgrthPpT_QEIJ28QzkCUHGFxXHabsMp-74WVnV9QcetbMSX1u9tG_T5VbbgwFSEkcHYRIm_zNqdNTRSDpwDSjlv2zbxss6H5SKb8_H2WbZ5XIVnzvpau_B8wJcc2n3bwJY9R3YFqwCMEu63aoaG7KOlkVTzYN9EuEOpnvMFqaEWmgbZUH8LnDomqukvSz7hEywIEOx9Jv81CB7aRUksytmaHKKQ"})

@app.route('/token',  methods=['GET', 'POST'])
def token():
   code = request.args.get("code")
   print(code)
   client = OAuthClient(
        token_url=DSP_token_url,
        client_id=DSP_client_id,
        client_secret=DSP_client_secret,
        code=code
    )
   session['access_token'] = client.get_access_token();
   #return redirect(url_for("menu"))
    
   print(jsonify({"access_token" : client.get_access_token()}));
   return jsonify({"access_token" : client.get_access_token()})
   #return client.get_access_token()

@app.route("/login",  methods=['GET', 'POST'])
def login():
    # SAP OAuth 2.0 Authorization URL
    auth_url = (
        "https://crl-np-dsp.authentication.us10.hana.ondemand.com/oauth/authorize"
        "?response_type=code"
        "&client_id=sb-60a4173c-2fa1-4a0f-ad14-5cd225c9cc03!b337428%7Cclient!b655"       
    )
    return redirect(auth_url)


if __name__ == '__main__':
    app.run(port=5000, host='0.0.0.0') 
