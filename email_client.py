import os
import imaplib
import email
import datetime
from email.header import decode_header, make_header
from email.utils import parseaddr
from dotenv import load_dotenv
from invoice_detector import is_invoice_attachment
from storage_local import upload_invoice
from document_pipeline import process_saved_invoice_file
from ingress_tracking import register_ingress_source_document_best_effort

load_dotenv()

IMAP_HOST = os.getenv("IMAP_HOST")
IMAP_PORT = int(os.getenv("IMAP_PORT", "993"))
EMAIL_USERNAME = os.getenv("EMAIL_USERNAME")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

TARGET_SENDERS = [
    s.strip().lower()
    for s in os.getenv("TARGET_SENDERS", "").split(",")
    if s.strip()
]

MAX_ATTACHMENT_SIZE_MB = 20
LOOKBACK_DAYS = 30


def _decode_str(value: str) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


def _connect_imap():
    if not IMAP_HOST or not EMAIL_USERNAME or not EMAIL_PASSWORD:
        raise RuntimeError("IMAP_HOST, EMAIL_USERNAME, and EMAIL_PASSWORD must be set in .env")
    imap = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    imap.login(EMAIL_USERNAME, EMAIL_PASSWORD)
    return imap


def fetch_and_process_invoices():
    logs = []

    try:
        imap = _connect_imap()
    except Exception as e:
        return [f"IMAP connection failed: {e}"]

    try:
        status, _ = imap.select("INBOX")
        if status != "OK":
            return [f"Failed to select INBOX: {status}"]

        since_date = (datetime.date.today() - datetime.timedelta(days=LOOKBACK_DAYS)).strftime("%d-%b-%Y")
        status, data = imap.search(None, "SINCE", since_date)
        if status != "OK":
            return [f"IMAP search failed: {status} {data}"]

        msg_ids = data[0].split()
        logs.append(f"Found {len(msg_ids)} messages since {since_date} in INBOX.")

        total_attachments = 0
        uploaded_count = 0
        skipped_non_target_sender = 0
        skipped_non_invoice = 0
        skipped_duplicates = 0

        for msg_id in msg_ids:
            msg_id_str = msg_id.decode(errors="ignore")
            try:
                status, msg_data = imap.fetch(msg_id, "(RFC822)")
                if status != "OK":
                    logs.append(f"Failed to fetch message {msg_id_str}: {status}")
                    continue

                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email)
                message_id = (msg.get("Message-ID") or "").strip()
                subject = _decode_str(msg.get("Subject"))
                from_header = _decode_str(msg.get("From"))
                from_name, from_email = parseaddr(from_header)
                from_email_lower = (from_email or "").lower()

                if TARGET_SENDERS and from_email_lower not in TARGET_SENDERS:
                    skipped_non_target_sender += 1
                    continue

                for part in msg.walk():
                    content_disposition = part.get_content_disposition()
                    filename = part.get_filename()
                    if content_disposition not in ("attachment", "inline") and not filename:
                        continue

                    filename = _decode_str(filename)
                    content_type = part.get_content_type()

                    try:
                        payload = part.get_payload(decode=True)
                    except Exception:
                        payload = None

                    if not payload:
                        continue

                    if len(payload) > MAX_ATTACHMENT_SIZE_MB * 1024 * 1024:
                        logs.append(
                            f"Skipping large attachment (> {MAX_ATTACHMENT_SIZE_MB}MB) "
                            f"{filename} from {from_email_lower}"
                        )
                        continue

                    total_attachments += 1

                    if not is_invoice_attachment(subject, filename, content_type):
                        skipped_non_invoice += 1
                        continue

                    try:
                        full_path, uploaded = upload_invoice(from_email_lower, message_id, filename, payload)
                        if uploaded:
                            uploaded_count += 1
                            logs.append(f"Saved {filename} from {from_email_lower} to {full_path}")
                            source_document, registration_warning = register_ingress_source_document_best_effort(
                                source_type="email_attachment",
                                storage_path=full_path,
                                source_ref=message_id or f"imap:{msg_id_str}",
                                original_filename=filename,
                                content_type=content_type,
                                content_bytes=payload,
                                from_email=from_email_lower,
                                email_message_id=message_id or None,
                                metadata={
                                    "email": {
                                        "subject": subject,
                                        "from_name": from_name,
                                        "imap_message_ref": msg_id_str,
                                        "content_disposition": content_disposition,
                                    }
                                },
                            )
                            if source_document:
                                logs.append(f"Registered source document {source_document['id']} for {filename}")
                            elif registration_warning:
                                logs.append(
                                    f"Source document registration skipped for {filename}: {registration_warning}"
                                )
                            result = process_saved_invoice_file(
                                full_path,
                                from_email=from_email_lower,
                                message_id=message_id,
                                original_filename=filename,
                                source_document_id=source_document["id"] if source_document else None,
                            )
                            logs.extend(result.get("logs") or [])
                        else:
                            skipped_duplicates += 1

                    except Exception as e:
                        logs.append(f"Failed to save {filename} from {from_email_lower}: {e}")

            except Exception as e:
                logs.append(f"Error processing message {msg_id_str}: {e}")

        logs.append(
            f"Summary: uploaded={uploaded_count}, "
            f"total_attachments_seen={total_attachments}, "
            f"skipped_non_target_sender={skipped_non_target_sender}, "
            f"skipped_non_invoice={skipped_non_invoice}, "
            f"skipped_duplicates={skipped_duplicates}"
        )

    finally:
        try:
            imap.close()
        except Exception:
            pass
        try:
            imap.logout()
        except Exception:
            pass

    return logs
