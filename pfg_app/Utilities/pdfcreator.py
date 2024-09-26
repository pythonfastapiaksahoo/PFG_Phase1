import os
from datetime import datetime

import pytz as tz
from reportlab.lib import colors
from reportlab.pdfgen import canvas

tz_region_name = os.getenv("serina_tz", "Asia/Dubai")
tz_region = tz.timezone(tz_region_name)


def createdoc(all_status, doc_data):
    pageslist = [all_status[i : i + 8] for i in range(0, len(all_status), 8)]
    filenames = []
    i = 0
    for page in pageslist:
        filename = "Invoice#" + str(i) + "_JourneyMap.pdf"
        image = "Utilities/serinaimg.jpg"
        title = doc_data["Entity"]
        subtitle = "Invoice Journey Document"
        pdf = canvas.Canvas(filename)
        if i == 0:
            pdf.drawImage(image, 10, 780, 120, 50)
            pdf.setFillColor(colors.black)
            pdf.setFont("Courier-Bold", 17)
            pdf.drawCentredString(290, 750, title)
            pdf.setFont("Courier-Bold", 14)
            pdf.drawCentredString(290, 730, subtitle)
            text = pdf.beginText(40, 700)
            text.setFont("Courier-Bold", 12)
            text.setFillColor(colors.black)
            text.textLine("Invoice Number: " + doc_data["InvoiceNumber"])
            text.textLine("Invoice Date: " + doc_data["InvoiceDate"])
            text.textLine("PO #: " + doc_data["PO"])
            text.textLine("GRN #: " + doc_data["GRN"])
            text.textLine("Entity: " + doc_data["Entity"])
            text.textLine("Vendor/Service Provider: " + doc_data["Vendor"])
            text.textLine(
                "Date/Time: "
                + datetime.utcnow().strftime("%d-%m-%Y %H:%M:%S %p")
                + " UTC"
            )
            pdf.drawText(text)
            startX = 40
            startY = 590
        else:
            startX = 40
            startY = 800
        dslogo = "Utilities/ds-logo.png"
        pdf.drawImage(dslogo, 10, 10, 50, 30)
        for s in page:
            text = pdf.beginText(startX, startY)
            text.setFont("Courier-Bold", 10)
            color = colors.green
            if s.dochistorystatus == "Invoice Uploaded":
                color = colors.blue
            elif (
                s.dochistorystatus == "OCR Error Found"
                or s.DocumentHistoryLogs.documentdescription
                == "Error in posting the invoice, but invoice saved as Pending invoice"
            ):
                color = colors.red
            text.setFillColor(color)
            if s.dochistorystatus == "":
                text.textline("\u2022 GRN Created in ERP & " + s.dochistorystatus)
            else:
                text.textLine(
                    "\u2022 " + s.dochistorystatus
                    if s.dochistorystatus is not None
                    else "\u2022 " + s.DocumentHistoryLogs.documentdescription
                )
            dt = s.DocumentHistoryLogs.CreatedOn
            pdf.drawText(text)
            text.setFillColor(colors.black)
            text.setFont("Courier", 10)
            text.textLine(
                "Done By: " + s.firstName
                if s.firstName is not None
                else "" + " " + s.lastName if s.lastName is not None else ""
            )
            text.textLine("Date & Time: " + dt.strftime("%d-%m-%Y %H:%M:%S %p"))
            text.setLeading(10)
            pdf.drawText(text)
            startY -= 60
        pdf.save()
        filenames.append(filename)
        i += 1
    return filenames
