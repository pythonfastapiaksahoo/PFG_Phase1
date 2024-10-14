from datetime import datetime

from reportlab.lib import colors
from reportlab.pdfgen import canvas


def createdoc(all_status, docid):
    filename = "Invoice#" + str(docid) + "_JourneyMap.pdf"
    image = "pfg_app/FROps/pfg-logo.png"
    subtitle = "Pattinson Food Group - Invoice Journey Document"
    pdf = canvas.Canvas(filename)
    pdf.drawImage(image, 10, 780, 120, 50)
    pdf.setFillColor(colors.black)
    pdf.setFont("Courier-Bold", 14)
    pdf.drawCentredString(290, 730, subtitle)
    startX = 40
    startY = 590
    invoiceId = ""
    item_count = 0  # Initialize item count
    items_per_page = 8  # Define how many items per page
    i = 0
    for s in all_status:
        if (
            item_count == items_per_page
        ):  # Check if the limit for the current page is reached
            pdf.showPage()  # Move to the next page
            item_count = 0  # Reset item count
            i += 1
            if i >= 1:
                startY = 750
                items_per_page = 12
        invoiceId = s.docheaderID
        invoicetype = s.UploadDocType
        confirmation_number = s.JournalNumber
        doc_date = s.documentDate
        vendor = s.VendorName
        if i == 0:
            text = pdf.beginText(40, 700)
            text.setFont("Courier-Bold", 12)
            text.setFillColor(colors.black)
            text.textLine("Invoice Number: " + invoiceId)
            text.textLine("Invoice Date: " + doc_date)
            text.textLine("Confirmation #: " + confirmation_number)
            text.textLine("invoice Type: " + invoicetype)
            text.textLine("Vendor/Service Provider: " + vendor)
            text.textLine(
                "Document Journey GeneratedDate/Time: "
                + datetime.utcnow().strftime("%d-%m-%Y %H:%M:%S %p")
                + " UTC"
            )
            pdf.drawText(text)
        text = pdf.beginText(startX, startY)
        text.setFont("Courier-Bold", 10)
        color = colors.green
        if s.DocumentHistoryLogs.documentStatusID in [2, 7, 14]:
            color = colors.green
        elif s.DocumentHistoryLogs.documentStatusID in [21]:
            color = colors.red
        else:
            color = colors.blue
        text.setFillColor(color)
        text.textLine(s.DocumentHistoryLogs.documentdescription)
        dt = s.DocumentHistoryLogs.CreatedOn
        pdf.drawText(text)
        text.setFillColor(colors.black)
        text.setFont("Courier", 10)
        if dt:
            text.textLine(
                "Date & Time: " + dt.strftime("%d-%m-%Y %H:%M:%S %p") + " UTC"
            )
        else:
            text.textLine("Date & Time:")
        pdf.drawText(text)
        startY -= 60
        item_count += 1
    pdf.save()
    return filename
