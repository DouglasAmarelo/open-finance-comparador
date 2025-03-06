"""
This script will get the data from the API and print it to the console
"""

import requests
import pandas as pd

# Lista de URLs das APIs
OF_APIS = {
    "https://api.itau/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://opendata.api.nubank.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://resources.openbanking.picpay.com/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://api.mercadopago.com/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://api-openbanking.bancopan.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://api.safra.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://banking-openfinance.xpi.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://banking-openfinance.xpi.com.br/rico/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://banking-openfinance-azimutbrasil.xpi.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://banking-openfinance.xpi.com.br/clear/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://banking-openfinance.xpi.com.br/modal/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://banking-openfinance-montebravo.xpi.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://banking-openfinance-whg.xpi.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://api-openbanking.bvopen.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://api.sicredi.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://api-opf.asa.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://ob-api.sisprimedobrasil.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://authorization.openapi.sofisa.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://api.e-unicred.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://btgmais.openbanking.btgpactual.com/open-banking/opendata-creditcards/v1/personal-credit-cards",
    "https://api.openfinance.bnb.gov.br/open-banking/opendata-creditcards/v1/personal-credit-cards",
    # "https://openbanking.api.santander.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",  # Erro!
    # "https://openbanking.bib.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",  # Erro!
    # "https://opendata.api.bb.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",  # Erro!
    # "https://api-bmg.bancobmg.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",  # Erro!
    # "https://api.bradesco.com/next/open-banking/opendata-creditcards/v1/personal-credit-cards",  # Erro!
    # "https://api.bradesco.com/bradesco/open-banking/opendata-creditcards/v1/personal-credit-cards",  # Erro!
    # "https://openbanking.banrisul.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",  # Erro!
    # "https://openfinance.sicoob.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",  # Erro!
    # "https://api.openbanking.caixa.gov.br/open-banking/opendata-creditcards/v1/personal-credit-cards",  # Erro!
    # "https://api.bradesco.com/bradescard/open-banking/opendata-creditcards/v1/personal-credit-cards",  # Erro!
    # "https://apps.neon.com.br/open-finance/open-banking/opendata-creditcards/v1/personal-credit-cards",  # Erro!
    # "https://openbanking.digio.com.br/open-banking/opendata-creditcards/v1/personal-credit-cards",  # Erro!
}


def get_data(api_url):
    """
    Função para obter os dados da API
    :param api_url: URL da API
    :return: Dados da API
    """

    response = requests.get(api_url, params={"page": 1, "page-size": 25}, timeout=100)

    if response.status_code != 200:
        print(f"Failed to get data: {response.status_code}")
        return None

    return response.json()["data"]


def process_cards_data(cards_data):
    """
    Função para processar os dados dos cartões de crédito
    :param cards_data: Dados dos cartões de crédito
    :return: Dicionário com as informações processadas
    """

    if not cards_data:
        return None

    # Estruturando as informações principais de 'participant', 'name', 'fees' e 'interest'
    processed_cards_data = []  # Lista para armazenar os dados principais
    processed_fees_data = []  # Lista para armazenar as tarifas
    processed_interest_data = []  # Lista para armazenar as taxas de juros

    # Iterando pelos dados do cartão
    for card in cards_data:
        # Informações principais do cartão
        card_info = {
            "brand": card["participant"]["brand"],
            "name_participant": card["participant"]["name"],
            "cnpj": card["participant"]["cnpjNumber"],
            "card_name": card["name"],
            "product_type": card["identification"]["product"]["type"],
            "credit_card_network": card["identification"]["creditCard"]["network"],
            "has_rewards_program": card["rewardsProgram"]["hasRewardProgram"],
        }

        # Adicionando os dados principais à lista `processed_data`
        processed_cards_data.append(card_info)

        # Extração das tarifas (fees)
        fees = card["fees"]["services"]
        for fee in fees:
            for price in fee["prices"]:
                fee_data = {
                    "service_name": fee["name"],
                    "service_code": fee["code"],
                    "interval": price["interval"],
                    "fee_value": price["value"],
                    "currency": price["currency"],
                    "customer_rate": price["customers"]["rate"],
                }
                # Adicionando as tarifas a lista, associadas aos dados principais do cartão
                processed_fees_data.append({**card_info, **fee_data})

        # Extração das taxas de juros (interest rates)
        interest_rates = card["interest"]["rates"]
        for interest in interest_rates:
            for application in interest["applications"]:
                processed_interest_data.append(
                    {
                        **card_info,
                        "referential_rate": interest["referentialRateIndexer"],
                        "interest_rate": interest["rate"],
                        "interval": application["interval"],
                        "indexer_rate": application["indexer"]["rate"],
                        "customer_rate": application["customers"]["rate"],
                    }
                )

    # return processed_data
    return processed_cards_data, processed_fees_data, processed_interest_data


def combine_data_from_api(api_list):
    """
    Função para combinar os dados de várias APIs
    :param api_list: Lista de URLs da API
    :return: Dados combinados
    """

    # Lista para armazenar todos os dados
    all_processed_cards_data = []
    all_fees_data = []
    all_interest_data = []

    # Iterando sobre a lista de APIs e processando os dados
    for api_url in api_list:
        cards_data = get_data(api_url)
        processed_cards_data, processed_fees_data, processed_interest_data = (
            process_cards_data(cards_data)
        )
        all_processed_cards_data.extend(processed_cards_data)
        all_fees_data.extend(processed_fees_data)
        all_interest_data.extend(processed_interest_data)

    return all_processed_cards_data, all_fees_data, all_interest_data


def combine_into_dataframe(processed_data):
    """
    Função para combinar os dados processados em um DataFrame
    :param processed_data: Dados processados
    :return: DataFrame
    """

    all_processed_cards_data, all_fees_data, all_interest_data = processed_data

    # Convertendo as listas em DataFrames: Tarifas, Taxas de juros e Informações principais do cartão
    cards_df = pd.DataFrame(all_processed_cards_data)
    fees_df = pd.DataFrame(all_fees_data)
    interest_df = pd.DataFrame(all_interest_data)

    # Exibindo as três informações no mesmo DataFrame
    # Para combinar as informações, vamos garantir que a coluna de 'card_name' seja a chave comum para as junções
    # Vamos primeiro expandir os dados principais para as tarifas e taxas de juros
    fees_df_combined = fees_df.merge(
        cards_df,
        on=[
            "brand",
            "name_participant",
            "cnpj",
            "card_name",
            "product_type",
            "credit_card_network",
            "has_rewards_program",
        ],
        how="left",
    )
    interest_df_combined = interest_df.merge(
        cards_df,
        on=[
            "brand",
            "name_participant",
            "cnpj",
            "card_name",
            "product_type",
            "credit_card_network",
            "has_rewards_program",
        ],
        how="left",
    )

    # Agora, vamos combinar os DataFrames de tarifas e taxas de juros
    combined_df = pd.concat([fees_df_combined, interest_df_combined], ignore_index=True)

    return combined_df


def export_to_csv(dataframe, filename):
    """
    Função para exportar o DataFrame para um arquivo CSV
    :param dataframe: DataFrame a ser exportado
    :param filename: Nome do arquivo CSV
    """

    dataframe.to_csv(filename, index=False)


def main():
    """
    Função principal
    """

    # Verificando se há URLs de API fornecidas
    if not OF_APIS:
        print("No API URLs provided.")
        return

    # Exibindo o DataFrame combinado
    combined_data = combine_data_from_api(OF_APIS)
    combined_df = combine_into_dataframe(combined_data)

    print(combined_df)
    # display(combined_df)

    # Exportando o DataFrame para um arquivo CSV
    export_to_csv(combined_df, "credit_card_data.csv")


if __name__ == "__main__":
    main()
