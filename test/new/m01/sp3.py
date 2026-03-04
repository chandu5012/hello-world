import os
import subprocess

# ------------------------------------------------------------
# 0) EMAIL HEADER (keep your existing header variables)
# ------------------------------------------------------------
# These should already exist in your code:
# MAIL_ID_FROM, to_mail_id, TO_MAIL, FROM_MAIL, MAIL_SUBJECT, html_content, log, get_timestamp()

email_header = "{}\n{}\n{}\nMIME-Version: 1.0\nContent-Type: text/html; charset=UTF-8\n".format(
    FROM_MAIL, TO_MAIL, MAIL_SUBJECT
)

# ------------------------------------------------------------
# 1) SET ATTACHMENT FILE PATH (ANY TYPE)
# ------------------------------------------------------------
# IMPORTANT: set this to your actual output file path.
# Examples:
# attach_path = "/tmp/report.csv"
# attach_path = "/tmp/report.xlsx"
# attach_path = "/tmp/query.sql"
# attach_path = "/tmp/report.html"
attach_path = self.OUTPUT_FILE_PATH   # <-- CHANGE THIS LINE to your real file path variable

# ------------------------------------------------------------
# 2) CHECK ATTACHMENT SIZE (<= 10MB)
# ------------------------------------------------------------
MAX_BYTES = 10 * 1024 * 1024  # 10MB
attach_file = False

if attach_path and os.path.exists(attach_path):
    size_bytes = os.path.getsize(attach_path)
    log.info("INFO ======> Attachment file: {} size={} bytes".format(attach_path, size_bytes))
    if size_bytes <= MAX_BYTES:
        attach_file = True
    else:
        log.info("INFO ======> Attachment skipped (file > 10MB)")
else:
    log.info("INFO ======> Attachment skipped (file not found): {}".format(attach_path))

# ------------------------------------------------------------
# 3) BUILD EMAIL MESSAGE
#    - Body is always html_content
#    - Attach file only if attach_file=True
# ------------------------------------------------------------
if attach_file:
    # Py3 imports
    try:
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders
    except ImportError:
        # Py2.7 imports
        from email.MIMEMultipart import MIMEMultipart
        from email.MIMEText import MIMEText
        from email.MIMEBase import MIMEBase
        from email import Encoders as encoders

    msg = MIMEMultipart()
    msg["From"] = MAIL_ID_FROM
    msg["To"] = to_mail_id
    msg["Subject"] = "Data Validation Report Process Completed - {}".format(get_timestamp())

    # Body (HTML)
    msg.attach(MIMEText(html_content, "html"))

    # Attachment (any type)
    part = MIMEBase("application", "octet-stream")
    with open(attach_path, "rb") as f:
        part.set_payload(f.read())

    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        'attachment; filename="{}"'.format(os.path.basename(attach_path))
    )
    msg.attach(part)  # <-- ACTUAL ATTACHMENT

    message = msg.as_string()
    log.info("INFO ======> Sending email WITH attachment: {}".format(os.path.basename(attach_path)))

else:
    # No attachment -> send only body (your original behavior)
    message = email_header + "\n" + html_content
    log.info("INFO ======> Sending email WITHOUT attachment")

# ------------------------------------------------------------
# 4) SEND USING /usr/lib/sendmail
# ------------------------------------------------------------
try:
    message_bytes = message.encode("utf-8")
except:
    message_bytes = message

mail_cmd = "/usr/lib/sendmail -t"
process = subprocess.Popen(
    mail_cmd,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    shell=True
)

out, err = process.communicate(message_bytes)

if process.returncode != 0:
    log.info("ERROR ======> sendmail failed rc={}, stderr={}".format(process.returncode, err))
    raise Exception("sendmail failed")
else:
    log.info("INFO ======> Email sent successfully. Attachment: {}".format("YES" if attach_file else "NO"))
