import logging
import unicodedata
from fuzzywuzzy import process
from binance_messages import send_text_message, send_messages
from binance_blacklist import add_to_blacklist
from lang_utils import payment_concept, payment_warning, anti_fraud_stage3, anti_fraud_not_valid_response, anti_fraud_possible_fraud, anti_fraud_user_denied
from binance_bank_deposit import get_payment_details
from common_vars import NOT_ACCEPTED_BANKS, ACCEPTED_BANKS
from binance_db_set import update_buyer_bank, update_anti_fraud_stage, update_kyc_status

logger = logging.getLogger(__name__)

def normalize_string(input_str):
    # Normalize the string to NFD form which separates characters and their accents
    normalized_str = unicodedata.normalize('NFD', input_str)
    # Filter out the non-spacing marks (accents)
    return ''.join([c for c in normalized_str if unicodedata.category(c) != 'Mn'])

async def handle_anti_fraud(buyer_name, seller_name, conn, anti_fraud_stage, response, order_no, connection_manager):
    questions = [
        f"¿Esta usted comprando porque le han ofrecido empleo, inversión con altos retornos o promesas de ganancias a cambio de que usted les envie estas criptomonedas? (1/3)",
        "¿Siente presión o urgencia inusual por parte de alguien para completar este pago de inmediato? (2/3)",
        f"¿Está usted de acuerdo que una vez completada la orden({order_no}) exitosamente, no hay posibilidad de reembolso o devolucion por parte del vendedor? (3/3)",
        "Muchas gracias por completar las preguntas, ahora para brindarle un servicio más eficiente, ¿podría indicarnos el nombre del banco que utilizará para realizar el pago?",
        f"Perfecto si aceptamos su banco. Por ultimo, la cuenta bancaria que utilizará para realizar el pago, ¿está a su nombre? ({buyer_name})",
    ]


    normalized_response = normalize_string(response.strip().lower())
    if anti_fraud_stage >= len(questions):
        return  # No more questions

    if anti_fraud_stage == 3:
        # Direct match for not accepted banks
        if normalized_response in [bank.lower() for bank in NOT_ACCEPTED_BANKS]:
            await connection_manager.send_text_message(anti_fraud_stage3, order_no)
            await add_to_blacklist(conn, buyer_name, order_no, None, normalized_response, anti_fraud_stage)
            return

        # Fuzzy matching for accepted banks
        closest_match, similarity = process.extractOne(normalized_response, [bank.lower() for bank in ACCEPTED_BANKS])
        if similarity >= 80:  # Threshold of 80%
            await update_buyer_bank(conn, order_no, closest_match)  # Use the closest match
        else:
            accepted_banks_list = ', '.join(ACCEPTED_BANKS)
            await connection_manager.send_text_message(f"No pudimos verificar el banco proporcionado. Por favor, asegúrese de elegir uno de los siguientes bancos aceptados: {accepted_banks_list}", order_no)
            await connection_manager.send_text_message(questions[3], order_no)  # Ask the bank question again
            return
        
    if anti_fraud_stage in [0, 1, 2, 4] and normalized_response not in ['si', 'no']:
        await connection_manager.send_text_message(anti_fraud_not_valid_response, order_no)
        await connection_manager.send_text_message(questions[anti_fraud_stage], order_no)
        return

    fraud_responses = {(0, 'si'), (1, 'si')}
    deny_responses = {(2, 'no'), (4, 'no')}

    if (anti_fraud_stage, normalized_response) in fraud_responses:
        await connection_manager.send_text_message(anti_fraud_possible_fraud, order_no)
        await add_to_blacklist(conn, buyer_name, order_no, None, normalized_response, anti_fraud_stage)
        return

    if (anti_fraud_stage, normalized_response) in deny_responses:
        await connection_manager.send_text_message(anti_fraud_user_denied, order_no)
        await add_to_blacklist(conn, buyer_name, order_no, None, normalized_response, anti_fraud_stage)
        return

    anti_fraud_stage += 1  # Proceed to the next stage
    await update_anti_fraud_stage(conn, buyer_name, anti_fraud_stage)

    if anti_fraud_stage == len(questions):
        await update_kyc_status(conn, buyer_name, 1)
        payment_details = await get_payment_details(conn, order_no, buyer_name)
        await send_messages(connection_manager, order_no, [payment_warning, payment_concept, payment_details])
    else:
        await connection_manager.send_text_message(questions[anti_fraud_stage], order_no)
