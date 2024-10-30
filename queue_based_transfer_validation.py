import asyncio
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List
import logging
from TEST_binance_cep import validate_transfer

logger = logging.getLogger(__name__)

@dataclass
class TransferValidationTask:
    clave_rastreo: str
    emisor: str
    receptor: str
    monto: float
    fecha: datetime
    last_tried: datetime
    retry_count: int
    order_no: str
    account_number: str

class TransferValidationQueue:
    def __init__(self):
        self.queue: List[TransferValidationTask] = []

    async def add_task(self, task: TransferValidationTask):
        self.queue.append(task)

    async def get_next_task(self) -> TransferValidationTask:
        now = datetime.now()
        for i, task in enumerate(self.queue):
            if now - task.last_tried >= timedelta(seconds=30 * (2 ** task.retry_count)):
                return self.queue.pop(i)
        return None

class TransferValidator:
    def __init__(self, queue: TransferValidationQueue, connection_manager):
        self.queue = queue
        self.connection_manager = connection_manager

    async def process_queue(self):
        while True:
            task = await self.queue.get_next_task()
            if task:
                await self.process_task(task)
            await asyncio.sleep(1)  # Avoid tight loop

    async def process_task(self, task: TransferValidationTask):
        try:
            validation_successful = await validate_transfer(
                task.fecha, task.clave_rastreo, task.emisor, task.receptor,
                task.account_number, task.monto
            )
            
            if validation_successful:
                logger.info(f"Transfer validation successful for order {task.order_no}")
                await self.connection_manager.send_text_message(
                    task.account_number, 
                    "Transfer validated successfully.", 
                    task.order_no
                )
            else:
                logger.warning(f"Transfer validation failed for order {task.order_no}, attempt {task.retry_count + 1}")
                if task.retry_count < 5:  # Max 6 attempts (0-5)
                    task.retry_count += 1
                    task.last_tried = datetime.now()
                    await self.queue.add_task(task)
                    await self.connection_manager.send_text_message(
                        task.account_number,
                        f"Transfer validation in progress. Retry {task.retry_count} scheduled.",
                        task.order_no
                    )
                else:
                    logger.error(f"Max retries reached for order {task.order_no}. Transfer validation ultimately failed.")
                    await self.connection_manager.send_text_message(
                        task.account_number,
                        "Transfer validation failed after multiple attempts. Please check your transfer details and try again later.",
                        task.order_no
                    )
        except Exception as e:
            logger.error(f"An error occurred during transfer validation for order {task.order_no}: {str(e)}")
            await self.connection_manager.send_text_message(
                task.account_number,
                "An error occurred during transfer validation. Please try again later.",
                task.order_no
            )