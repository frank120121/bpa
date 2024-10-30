# returning_customer.py
import logging
import asyncio
import unicodedata
import traceback
from typing import Optional, List
from binance_messages import send_messages
from binance_blacklist import add_to_blacklist
from lang_utils import anti_fraud_stage3, anti_fraud_not_valid_response, anti_fraud_user_denied, CUSTOMER_VERIFICATION_MESSAGES, BankMessageTemplate
from common_vars import NOT_ACCEPTED_BANKS, ACCEPTED_BANKS, normalize_bank_name
from binance_db_set import update_buyer_bank, update_returning_customer_stage

logger = logging.getLogger(__name__)

def normalize_string(input_str: str) -> str:
    """Normalize string by removing diacritics and converting to lowercase."""
    if not input_str:
        return ""
    try:
        normalized_str = unicodedata.normalize('NFD', input_str)
        return ''.join([c for c in normalized_str if unicodedata.category(c) != 'Mn']).lower()
    except Exception as e:
        logger.error(f"Error normalizing string: {str(e)}")
        return ""

async def handle_bank_verification(
    conn,
    normalized_response: str,
    buyer_name: str,
    account: str,
    order_no: str,
    connection_manager,
    questions: List[str],
    returning_customer_stage: int,
    is_new_customer: bool = False
) -> Optional[str]:
    """
    Handle the bank verification process for both new and returning customers.
    
    Args:
        conn: Database connection
        normalized_response: Normalized user input for bank name
        buyer_name: Name of the buyer
        account: Account identifier
        order_no: Order number
        connection_manager: Connection manager instance
        questions: List of verification questions
        returning_customer_stage: Current stage in the verification process
        is_new_customer: Boolean indicating if this is a new customer
        
    Returns:
        Optional[str]: Verified bank name if successful, None otherwise
    """
    try:
        if not normalized_response:
            logger.warning(f"Empty bank name provided for buyer {buyer_name}")
            return None

        standard_bank_name = normalize_bank_name(normalized_response)
        
        # Check if bank is in NOT_ACCEPTED_BANKS
        if standard_bank_name in [normalize_bank_name(bank) for bank in NOT_ACCEPTED_BANKS]:
            await connection_manager.send_text_message(account, anti_fraud_stage3, order_no)
            await add_to_blacklist(conn, buyer_name, order_no, None, standard_bank_name, returning_customer_stage)
            logger.info(f"Bank {standard_bank_name} is not accepted for buyer {buyer_name}")
            return None

        # Check if bank is in ACCEPTED_BANKS
        if standard_bank_name in [normalize_bank_name(bank) for bank in ACCEPTED_BANKS]:
            await update_buyer_bank(conn, buyer_name, standard_bank_name)
            
            # Calculate next stage and validate bounds
            next_stage = 1 if is_new_customer else returning_customer_stage + 1
            if next_stage >= len(questions):
                logger.error(f"Stage index out of bounds for buyer {buyer_name}: {next_stage}")
                return None
                
            await update_returning_customer_stage(conn, buyer_name, next_stage)
            
            # Get appropriate next question
            next_question = questions[next_stage]
            await connection_manager.send_text_message(account, next_question, order_no)
            logger.info(f"Bank {standard_bank_name} verified successfully for buyer {buyer_name}")
            return standard_bank_name
        
        # Handle invalid bank
        accepted_banks_list = ', '.join(ACCEPTED_BANKS)
        await connection_manager.send_text_message(
            account,
            CUSTOMER_VERIFICATION_MESSAGES["bank_verification_failed"](accepted_banks_list),
            order_no
        )
        await asyncio.sleep(2)
        
        # Use appropriate question based on customer type
        current_question = (
            CUSTOMER_VERIFICATION_MESSAGES["bank_request"]
            if is_new_customer
            else questions[returning_customer_stage]
        )
        await connection_manager.send_text_message(account, current_question, order_no)
        logger.warning(f"Invalid bank {standard_bank_name} provided by buyer {buyer_name}")
        return None

    except Exception as e:
        logger.error(f"Error in bank verification for buyer {buyer_name}: {str(e)}\n{traceback.format_exc()}")
        return None

def get_customer_questions(
    buyer_name: str,
    buyer_bank: Optional[str],
    is_new_customer: bool = False
) -> List[str]:
    """
    Generate verification questions based on customer context and type.
    
    Args:
        buyer_name: Name of the buyer
        buyer_bank: Optional bank name
        is_new_customer: Boolean indicating if this is a new customer
        
    Returns:
        List of appropriate verification questions
    """
    try:
        if is_new_customer:
            return [
                CUSTOMER_VERIFICATION_MESSAGES["bank_request"],
                CUSTOMER_VERIFICATION_MESSAGES["account_ownership"](buyer_name)
            ]
        
        return [
            CUSTOMER_VERIFICATION_MESSAGES["bank_confirmation"](buyer_bank) if buyer_bank else "",
            CUSTOMER_VERIFICATION_MESSAGES["bank_request"],
            CUSTOMER_VERIFICATION_MESSAGES["account_ownership"](buyer_name)
        ]
    except Exception as e:
        logger.error(f"Error generating questions for buyer {buyer_name}: {str(e)}")
        # Return safe fallback questions
        return [
            CUSTOMER_VERIFICATION_MESSAGES["bank_request"],
            CUSTOMER_VERIFICATION_MESSAGES["account_ownership"](buyer_name)
        ]

async def handle_invalid_response(
    connection_manager,
    account: str,
    order_no: str,
    questions: List[str],
    returning_customer_stage: int
) -> None:
    """Handle invalid yes/no responses."""
    try:
        await connection_manager.send_text_message(account, anti_fraud_not_valid_response, order_no)
        await asyncio.sleep(2)
        await connection_manager.send_text_message(account, questions[returning_customer_stage], order_no)
    except Exception as e:
        logger.error(f"Error handling invalid response for order {order_no}: {str(e)}")

async def handle_customer_denial(
    conn,
    buyer_name: str,
    order_no: str,
    normalized_response: str,
    returning_customer_stage: int,
    connection_manager,
    account: str
) -> None:
    """Handle cases where the customer is denied."""
    try:
        await connection_manager.send_text_message(account, anti_fraud_user_denied, order_no)
        await add_to_blacklist(conn, buyer_name, order_no, None, normalized_response, returning_customer_stage)
        logger.info(f"Customer {buyer_name} denied for order {order_no}")
    except Exception as e:
        logger.error(f"Error handling customer denial for buyer {buyer_name}: {str(e)}")

async def handle_existing_bank_confirmation(
    conn,
    buyer_name: str,
    order_no: str,
    payment_manager,
    connection_manager,
    account: str
) -> None:
    """Handle confirmation of existing bank details."""
    try:
        # Get and send payment details once
        payment_details = await payment_manager.get_payment_details(conn, order_no, buyer_name)
        await send_messages(connection_manager, account, order_no, [payment_details])
        
        # Update stage to completed
        returning_customer_stage = 3
        await update_returning_customer_stage(conn, buyer_name, returning_customer_stage)
        logger.info(f"Existing bank confirmed for buyer {buyer_name}")
        return  # Add return here to prevent any further processing
    except Exception as e:
        logger.error(f"Error handling bank confirmation for buyer {buyer_name}: {str(e)}")

async def returning_customer(
    buyer_name: str,
    conn,
    returning_customer_stage: int,
    response: str,
    order_no: str,
    connection_manager,
    account,
    payment_manager,
    buyer_bank: Optional[str] = None
) -> None:
    """
    Verify returning customer's bank information and provide payment details if valid.
    
    Flow:
    1. Stage 0: Confirm if using existing bank
       - If yes -> proceed to payment details
       - If no -> go to stage 1
    2. Stage 1: Get and verify new bank
       - If valid -> go to stage 2
       - If invalid -> stay at stage 1
    3. Stage 2: Confirm account ownership
       - If yes -> provide payment details
       - If no -> deny and blacklist
    """
    try:
        is_new_customer = buyer_bank is None
        questions = get_customer_questions(buyer_name, buyer_bank, is_new_customer)
        normalized_response = normalize_string(response.strip())

        logger.info(
            f"Processing returning customer {buyer_name}, "
            f"stage {returning_customer_stage}, response: '{normalized_response}', "
            f"new customer: {is_new_customer}"
        )
        # Early return for completion stage
        if returning_customer_stage >= len(questions):
            logger.info(f"Customer {buyer_name} already completed verification (stage {returning_customer_stage})")
            return

        # Handle empty initial response for all stages
        if not normalized_response:
            await connection_manager.send_text_message(
                account,
                questions[returning_customer_stage],
                order_no
            )
            return

        # Handle new customers (no bank on file)
        if is_new_customer:
            verified_bank = await handle_bank_verification(
                conn=conn,
                normalized_response=normalized_response,
                buyer_name=buyer_name,
                account=account,
                order_no=order_no,
                connection_manager=connection_manager,
                questions=questions,
                returning_customer_stage=0,
                is_new_customer=True
            )
            if not verified_bank:
                return

        # Validate stage bounds
        if returning_customer_stage >= len(questions):
            logger.warning(f"Invalid stage {returning_customer_stage} for buyer {buyer_name}")
            return

        # Stage 0: Initial bank confirmation
        if returning_customer_stage == 0:
            if normalized_response not in ['si', 'no']:
                await handle_invalid_response(
                    connection_manager=connection_manager,
                    account=account,
                    order_no=order_no,
                    questions=questions,
                    returning_customer_stage=returning_customer_stage
                )
                return

            if normalized_response == 'si':
                # Using same bank, proceed to payment details
                await handle_existing_bank_confirmation(
                    conn=conn,
                    buyer_name=buyer_name,
                    order_no=order_no,
                    payment_manager=payment_manager,
                    connection_manager=connection_manager,
                    account=account
                )
                return
            else:
                # Need new bank, move to stage 1
                returning_customer_stage = 1
                await update_returning_customer_stage(conn, buyer_name, returning_customer_stage)
                await connection_manager.send_text_message(
                    account,
                    questions[returning_customer_stage],
                    order_no
                )
                return

        # Stage 1: Bank verification
        if returning_customer_stage == 1:
            verified_bank = await handle_bank_verification(
                conn=conn,
                normalized_response=normalized_response,
                buyer_name=buyer_name,
                account=account,
                order_no=order_no,
                connection_manager=connection_manager,
                questions=questions,
                returning_customer_stage=returning_customer_stage,
                is_new_customer=False
            )
            if not verified_bank:
                return

            # Bank verified, move to stage 2
            returning_customer_stage = 2
            await update_returning_customer_stage(conn, buyer_name, returning_customer_stage)
            await connection_manager.send_text_message(
                account,
                questions[returning_customer_stage],
                order_no
            )
            return

        # Stage 2: Account ownership confirmation
        if returning_customer_stage == 2:
            if normalized_response not in ['si', 'no']:
                await handle_invalid_response(
                    connection_manager=connection_manager,
                    account=account,
                    order_no=order_no,
                    questions=questions,
                    returning_customer_stage=returning_customer_stage
                )
                return

            if normalized_response == 'no':
                await handle_customer_denial(
                    conn=conn,
                    buyer_name=buyer_name,
                    order_no=order_no,
                    normalized_response=normalized_response,
                    returning_customer_stage=returning_customer_stage,
                    connection_manager=connection_manager,
                    account=account
                )
                return

            # Account ownership confirmed, provide payment details
            payment_details = await payment_manager.get_payment_details(
                conn,
                order_no,
                buyer_name
            )
            await send_messages(connection_manager, account, order_no, [payment_details])
            returning_customer_stage = 3
            await update_returning_customer_stage(conn, buyer_name, returning_customer_stage)
            logger.info(f"Completed returning customer flow for buyer {buyer_name}")

    except Exception as e:
        logger.error(
            f"Error in returning customer flow for buyer {buyer_name}: "
            f"{str(e)}\n{traceback.format_exc()}"
        )