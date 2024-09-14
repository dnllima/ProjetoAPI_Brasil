import requests
import pandas as pd

url_apibrasil = "https://brasilapi.com.br/api/banks/v1"

payload = {}
headers = {}

response = requests.request("GET", url_apibrasil, headers=headers, data=payload)

if response.status_code ==200:
    data = response.json()

    df_brasil = pd.DataFrame(data)

    print (df_brasil)

else: 

 print(f'Erro de requisição: {response.status_code}')


df_brasil.columns