from reportlab.pdfgen import canvas
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.lib import colors
import pytz as tz
import os
from datetime import datetime
from pfg_app.schemas.pfgtriggerSchema import InvoiceVoucherSchema

def createdoc(all_status,docid):
    filename = "Invoice#"+str(docid)+"_JourneyMap.pdf"
    image = 'pfg_app/FROps/pfg-logo.png'
    subtitle = "Pattinson Food Group - Invoice Journey Document"
    pdf = canvas.Canvas(filename)
    pdf.drawImage(image, 10, 780,120,50)
    pdf.setFillColor(colors.black)
    pdf.setFont("Courier-Bold", 14)
    pdf.drawCentredString(290, 730, subtitle)
    startX = 40
    startY = 590
    invoiceId = ''
    for s in all_status:
        invoiceId = s.docheaderID
        invoicetype = s.UploadDocType
        confirmation_number = s.JournalNumber
        doc_date = s.documentDate
        vendor = s.VendorName
        text = pdf.beginText(startX,startY)
        text.setFont("Courier-Bold", 10)
        color = colors.green
        if s.DocumentHistoryLogs.documentStatusID in [2,7,14]:
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
        # text.textLine("Done By: "+ s.firstName if s.firstName is not None else ""+ " "+ s.lastName if s.lastName is not None else "")
        if dt:
            text.textLine("Date & Time: "+ dt.strftime('%d-%m-%Y %H:%M:%S %p')+" UTC")
        else:
            text.textLine("Date & Time:")
        text.setLeading(10)
        pdf.drawText(text)
        startY -= 60
    text = pdf.beginText(40,700)
    text.setFont("Courier-Bold", 12)
    text.setFillColor(colors.black)
    text.textLine("Invoice Number: "+ invoiceId)
    text.textLine("Invoice Date: "+ doc_date)
    text.textLine("Confirmation #: "+confirmation_number)
    text.textLine("invoice Type: "+ invoicetype)
    text.textLine("Vendor/Service Provider: "+ vendor)
    text.textLine("Document Journey GeneratedDate/Time: "+datetime.utcnow().strftime("%d-%m-%Y %H:%M:%S %p")+" UTC")
    pdf.drawText(text)
    pdf.save()
    return filename