from common_vars import ProhibitedPaymentTypes
import logging
from logging_config import setup_logging

# Setting up logging
setup_logging(log_filename='Binance_c2c_logger.log')
logger = logging.getLogger(__name__)

# Language determination function
def determine_language(order_details):
    lang_mappings = {'MXN': 'es', 'USD': 'en'}
    return lang_mappings.get(order_details.get('fiat_unit', 'en'))

# Async function to get menu for an order
async def get_menu_for_order(order_details):
    language = determine_language(order_details)
    return get_menu_by_language(language, order_details.get('order_status'))

# Async function to get default reply
async def get_default_reply(order_details):
    language = determine_language(order_details)
    return (
        "Si tiene alguna duda o necesita ayuda, solo escriba 'ayuda' en el chat y le presentaré un menú de opciones."
        if language == 'es' else
        "If you have any questions or need assistance, just type 'help' in the chat, and I'll present you with an options menu."
    )

# Async function to get response for a menu choice
async def get_response_for_menu_choice(language, status, choice, buyer_name=None):
    response = MENU_RESPONSES.get(status, {}).get(language, {}).get(choice)
    if response:
        if isinstance(response, list):
            response = "\n\n".join(
                msg.format(buyer_name=buyer_name, ProhibitedPaymentTypes=ProhibitedPaymentTypes) for msg in response
            )
        else:
            response = response.format(buyer_name=buyer_name, ProhibitedPaymentTypes=ProhibitedPaymentTypes)
    return response

# Async function to validate menu choice
async def is_valid_choice(language, status, choice):
    valid_choices = MENUS.get(status, {}).get(language, [])
    return choice in range(1, len(valid_choices) + 1)

# Async function to get reply for invalid choice
async def get_invalid_choice_reply(order_details):
    language = determine_language(order_details)
    return "Opción no válida." if language == 'es' else "Invalid choice."

# Global variables for status messages, menus, and menu responses
STATUS_MESSAGES, MENUS, MENU_RESPONSES = {}, {}, {}

# Function to get menu by language
def get_menu_by_language(language, status):
    return MENUS.get(status, {}).get(language, [])

# Async function to get message by language
async def get_message_by_language(language, status, buyer_name=None):
    messages = STATUS_MESSAGES.get(status, {}).get(language, None)
    if messages:
        if isinstance(messages, list):
            messages = [msg.format(buyer_name=buyer_name, ProhibitedPaymentTypes=ProhibitedPaymentTypes) for msg in messages]
        else:
            messages = [messages.format(buyer_name=buyer_name, ProhibitedPaymentTypes=ProhibitedPaymentTypes)]
    return messages

STATUS_MESSAGES = {
    2: {
        'es': "Si aun no envia el comprobante de pago por favor enviarlo ahora para poder liberar la orden(requerido). De lo contrario por favor ignorar este mensaje.",
        'en': "If you have not sent the proof of payment, please send it now so I can release the order(required). If you have already sent it, please ignore this message. For help type \"HELP\"."
    },
    1: {
        'es': [
            "**********IMPORTANTE!!!**********\n\n",
            "Hola {buyer_name}. Para recibir los datos de deposito es necesario confirmar que cumpla con los terminos del anuncio.",

        ],
        'en': [
            "Hello,{buyer_name}. For your safety, before procceding with the order, it is necessary to confirm you quilify for the terms and conditions of the ad.",
        ],
    },
    7: {
        'es': (
            "veo que la orden se cancelo. Si envio el pago y la orden se cancelo no se preocupe, puede abrir una nueva orden sin tener que pagar dos veces. "
            "Solo adjunte el comprobante de pago y seleccione la opción que dice 'realizar pago' o 'transferir notificar al vendedor' con gusto lo atendere. (respuesta automatica)"
        ),
        'en': "Order has been automatically cancelled. If you have sent the payment, please open a new order and attach your proof of your payment."
    },

    8: {
        'es': 'Listo el pago ya se envio por favor verificar el deposito en su cuenta y proceder a liberar la orden. Muchas gracias por su apoyo pase un excelente dia.',
        'en': 'The payment has been sent, please verify you have received the deposit and release the order. Thank you have a great day!'

    },
    9: {
        'es': (
            "Debido a que violó los términos del anuncio se procede con una apelacion de la orden.\n\n"
            "En caso de cualquier retorno, se require un lapso de hasta 24 horas para poder realizar el retorno.\n" 
            "En caso de retorno proporcione el número de CLABE INTERBANCARIA y nombre de BENEFICIARIO para poder proceder lo más rápido posible.\n\n"
            "Por favor no se desespere, realizaré el retorno en cuanto me sea posible. Tome en cuenta que tengo otras órdenes pendientes.\n\n"
            "Muchas gracias por su comprensión.(respuesta automatica)"
        ),
        'en': "Order is under appeal due to violation of terms. Please wait while the team resolves your order."
    },
    5: {
        'es': "Le agredeceria unos minutos para poder verificar el problema, sigo con usted.",
        'en': "Please allow me a few minutes to check the order."

    },
    6: {
        'es': "Esta orden ya esta cancelada, si require asistencia por favor abrir una nueva ordern o contactar a soporte.",
        'en': "This order is cancelled if you still reqiure assistance please open a new order please open a new order or contact support."
    }
    ,
    4: {
        'es': (
            "Activo depositado en su billetara de fondos. Por favor no olvide calificar esta transaccion.\n\n"
            "Muchas gracias por su apoyo pase un excelente dia 😊"
        ),
        'en': "This order is now completed, you can check in your funds or spot wallet please."
    }
    ,
    100: {
        'es': "Enseguida verifico.",
        'en': "One moment please, I will check."
    }
}
MENUS = {
    1: {
        'es': [
            "Le presento las opciones disponibles del 1-3:",
            "1. Opciones de pago.",
            "2. Que incluir en el concepto/referencia.",
            "3. Terminos de la orden.",
            "Por favor solo introducir el numero correspondiente."
        ],
        'en': [
            "1. Payment options.",
            "2. What to include in the concept/reference.",
            "3. Order terms."
        ]
    },
    2: {
        'es': [
            "Le presento las opciones disponibles del 1-5:",
            "",
            "1. Estado del pago.",
            "2. Cómo enviar prueba de pago.",
            "3. ¿Qué hacer si enviaste una cantidad incorrecta?",
            "4. ¿Necesitas más tiempo para realizar el pago?",
            "5. Tiempo de espera para transacciones BBVA.",
            "",
            "Por favor solo introducir el numero correspondiente."
        ],
        'en': [
            "1. Payment status.",
            "2. How to send payment proof.",
            "3. What to do if you sent the wrong amount?",
            "4. Need more time to make the payment?",
        ]
    }
}


MENU_RESPONSES = {
    1: {
        'es': {
            1: (
                "Para ver las opciones de pago por favor seleccione la opcion Realizar Pago en la parte superior > Seleccione el metodo de pago "
                "> Selecione la opcion que dice Transferido, notificar al vendedor despues de realizar el pago."
            ),
            2: " Para el concepto estas son opciones validas: pago, o su nombre ({buyer_name}).",
            3: "Pagos provenientes de {ProhibitedPaymentTypes} estan PROHIBIDOS y seran APELADOS."
        },
        'en': {
            1: "The payment options are X, Y, and Z.",
            2: "Please include ABC in the concept/reference of your payment.",
            3: "The terms of the order are: ..."
        }
    },
    2: { 
        'es': {
            1: "Estamos validando su pago, por favor permitame unos minutos para verificar. Gracias por su paciencia",
            2: "Puedes enviar la prueba de pago selecionando el boton (+) > ALBUM > y la captura que desea adjuntar.",
            3: "Si enviaste una cantidad incorrecta, deberas cancelar esta orden primero y despues abrir una nueva orden por la cantidad correcta.",
            4: (
                "Si necesitas más tiempo para realizar el pago, debido a que necesitas dar de alta la cuenta podemos esperar. "
                "Siempre y cuando no se demore mas de 1 hora, de lo contrario la orden sera APELADA."
            ),
            5: [
                "El tiempo de espera para transacciones BBVA puede llegar a ser hasta de 4 horas.",
                (
                    "Puede verificar el estado de la transferncia en su app BBVA ingresando a la app > selecione la cuenta desde donde realizo el pago "
                    "> En la parte de abajo seleccione la opcion: Ver todos > el pago que realizo > y entre la cantidad y la fecha la va aparecer "
                    "Establecido cuando el pago se haga realizado o en transito si aun no se envia."
                ),
                "Para pagos desde la web le va aparecer como liquidado cuando el pago sea enviado, de lo contrario aparece como pendiente.",
            ],
        },
        'en': {
            1: "Your payment is under review, please allow the team a few more minutes. Thank you for your patience.",
            2: "You can send the proof of payment via the (+) button > ALBUM > and select the correct screenshot.",
            3: "If you sent the wrong amount, please first cancel the order and then open a new order for the correct amount.",
            4: "If you need more time to make the payment, please cancel the order and open a new one once you are ready.",
        }
    }
}

transaction_denied = (
    "For security reasons, I cannot sell to you. Please cancel the order.\n\n"
    "Por razones de seguridad no es posible proceder. Por favor cancele la orden."
)

invalid_country = (
    "Lo sentimos, actualmente no estamos aceptando pagos de este país. Estamos trabajando constantemente para expandir la lista de países aceptados.\n\n"
    "Gracias por elegirnos, que tenga un excelente día.\n\n"
    "sorry, we are not currently accepting payments from this country. We are constantly working to expand the list of accepted countries.\n\n"
    "Thank you for choosing us, have a great day.\n\n"
    "\n\n"
    "En caso de que este utilizando un VPN, por favor desactívelo he intentarlo mas tarde.\n\n"
    "If you are using a VPN, please disable it and try again later."

)


anti_fraud_stage3 = (
    "Lo sentimos, actualmente no estamos aceptando pagos de este banco. Estamos trabajando constantemente para expandir la lista de bancos aceptados.\n\n"
    "Gracias por elegirnos, que tenga un excelente día."
)

anti_fraud_not_valid_response = (
    "Por favor responda exactamente con un 'Si' o un 'No' a la siguiente pregunta:"
)

anti_fraud_user_denied = (
    "Por razones de seguridad, no podemos continuar con este intercambio. Gracias por su comprensión."
)

anti_fraud_possible_fraud = (
    "Por razones de seguridad, no podemos continuar con este intercambio. Es posible que esté siendo víctima de un fraude. Por favor cancele la orden y no realice ninguna transferencia ya que puede perder su dinero."
)

payment_warning = (
    f"Pagos provenientes de {ProhibitedPaymentTypes} estan PROHIBIDOS y seran APELADOS.\n\n"
    f"Al enviar el pago, está confirmando que está de acuerdo con los términos y que toda responsabilidad del uso y pérdidas de los activos "
    f"después de este intercambio son únicamente suyas.\n\n"
    f"Es su responsabilidad realizar la investigación necesaria antes de cualquier inversión ya que las perdidas son reales e irreversibles.\n\n"
)

payment_concept = (
    f"Para el concepto estas son opciones validas: pago, o su nombre.\n\n"
    f"Para que no se cancele el intercambio de forma automática, puede marcar el intercambio como pagado en la opción que dice 'Realizar Pago'.\n\n" 
    f"Para ayuda, escriba la palabra 'ayuda'."
)

