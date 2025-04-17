import datetime
import traceback
import uuid
from pfg_app import model
from pfg_app.graph_api.message_processing import process_new_message
from pfg_app.logger_module import logger, set_operation_id
from pfg_app.session.session import get_db


def retry_unprocessed_corp_mails():
    try:
      # set_operation_id(uuid.uuid4().hex)
      operation_id = uuid.uuid4().hex  # Generate a new operation_id
      set_operation_id(operation_id)  # Set the new operation_id
      logger.info(f"Generated new operation ID: {operation_id}")  # Log the new operation_id
      db = next(get_db())
      # Calculate the timestamp for 2 days ago
      two_days_ago = datetime.datetime.utcnow() - datetime.timedelta(days=2)

      # Get all CorpMail records from the last two days
      recent_corp_mails = db.query(model.CorpMail).filter(model.CorpMail.created_at >= two_days_ago).all()

      # Prepare a set of existing mail_row_keys from CorpQueueTask for quick lookup
      existing_keys = {
          row[0] for row in db.query(model.CorpQueueTask.mail_row_key).all()
      }

      for mail in recent_corp_mails:
          mail_row_key = f"CE-{mail.id}"
          if mail_row_key not in existing_keys:
              logger.info(f"Retrying unprocessed mail ID: {mail.id}")
              process_new_message(mail.message_id, mail.id, operation_id)

    except Exception as e:
      logger.error(f"Failed to retry unprocessed CorpMail messages: {traceback.format_exc()}")