"""
This script will get the data from the API and print it to the console
"""

import requests
import pandas as pd

# API_BASE_URL = "https://resources.openbanking.picpay.com/open-banking/opendata-creditcards/v1/personal-credit-cards"
API_BASE_URL = "https://btgmais.openbanking.btgpactual.com/open-banking/opendata-creditcards/v1/personal-credit-cards"
API_PARAMS = {"page": 1, "page-size": 25}


def get_data(api, params):
    response = requests.get(api, params=params)

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
    return {
        "processed_cards_data": processed_cards_data,
        "processed_fees_data": processed_fees_data,
        "processed_interest_data": processed_interest_data,
    }


def main():
    """
    Função principal
    """

    cards_data = get_data(API_BASE_URL, API_PARAMS)
    cards_processed_data = process_cards_data(cards_data)

    # Convertendo as listas em DataFrames: Tarifas, Taxas de juros e Informações principais do cartão
    fees_df = pd.DataFrame(cards_processed_data["processed_fees_data"])
    interest_df = pd.DataFrame(cards_processed_data["processed_interest_data"])
    cards_df = pd.DataFrame(cards_processed_data["processed_cards_data"])

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

    # Exibindo o DataFrame combinado
    # print(combined_df)
    display(combined_df)


if __name__ == "__main__":
    main()
