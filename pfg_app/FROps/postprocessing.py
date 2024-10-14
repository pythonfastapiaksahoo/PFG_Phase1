import os
import re
import traceback
from collections import Counter

import pandas as pd
import pytz as tz

# SQL_DB = SCHEMA
from fuzzywuzzy import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import join

import pfg_app.model as model
from pfg_app.core.stampData import VndMatchFn
from pfg_app.logger_module import logger
from pfg_app.session.session import SCHEMA, SQLALCHEMY_DATABASE_URL

tz_region_name = os.getenv("serina_tz", "Asia/Dubai")
tz_region = tz.timezone(tz_region_name)


def date_cnv(doc_date, date_format):
    correctDate = None
    get_date = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12",
    }

    date_status = 0
    req_date = doc_date

    try:

        if date_format in [
            "mm.dd.yyyy",
            "mm-dd-yyyy",
            "mm/dd/yyyy",
            "mm dd yyyy",
            "mm.dd.yy",
            "mm/dd/yy",
            "mm dd yy",
            "mm/dd/yyyy",
            "mm/dd/yy",
            "mm.dd.yyyy",
            "mmm-dd-yyyy",
            "mmm dd yyyy",
        ]:
            doc_dt_slt = re.findall(r"\d+", doc_date)
            if len(doc_dt_slt) == 3:
                mm = doc_dt_slt[0]
                dd = doc_dt_slt[1]
                yy = doc_dt_slt[2]
                if len(dd) == 1:
                    dd = "0" + str(dd)
                if len(yy) == 2:
                    yy = "20" + str(yy)
                if len(mm) == 1:
                    mm = "0" + str(mm)
                req_date = yy + "-" + mm + "-" + dd
                date_status = 1

            elif len(doc_dt_slt) == 2:
                if doc_date[:3].lower() in list(get_date.keys()):
                    dd = doc_dt_slt[0]
                    yy = doc_dt_slt[1]
                    mm = get_date[doc_date[:3].lower()]
                    if len(dd) == 1:
                        dd = "0" + str(dd)
                    if len(yy) == 2:
                        yy = "20" + str(yy)
                    if len(mm) == 1:
                        mm = "0" + str(mm)
                    req_date = yy + "-" + mm + "-" + dd
                    date_status = 1
                else:
                    date_status = 0
                    req_date = doc_date
            else:
                date_status = 0
                req_date = doc_date
        elif date_format in [
            "dd-mm-yy",
            "dd mm yy",
            "dd.mm.yy",
            "dd.mm.yyyy",
            "dd-mm-yyyy",
            "dd mm yyyy",
            "dd/mm/yy",
            "dd mm yy",
            "dd mm yyyy",
            "dd.mm.yyyy",
            "dd/mm/yy",
            "dd-mmm-yy",
            "dd-mm-yyyy",
            "dd-mm-yy",
            "dd/mm/yyyy",
            "dd mmm yyyy",
        ]:
            doc_dt_slt = re.findall(r"\d+", doc_date)
            if len(doc_dt_slt) == 3:
                dd = doc_dt_slt[0]
                mm = doc_dt_slt[1]
                yy = doc_dt_slt[2]
                if len(dd) == 1:
                    dd = "0" + str(dd)
                if len(yy) == 2:
                    yy = "20" + str(yy)
                if len(mm) == 1:
                    mm = "0" + str(mm)
                req_date = yy + "-" + mm + "-" + dd
                date_status = 1
            elif len(doc_dt_slt) == 2:
                date_res = re.split(r"([-+]?\d+\.\d+)|([-+]?\d+)", doc_date.strip())
                res_f = [
                    r.strip() for r in date_res if r is not None and r.strip() != ""
                ]
                while "th" in res_f:
                    res_f.remove("th")
                for mnt_ck in range(len(res_f)):
                    while res_f[mnt_ck][0].isalnum() == 0:
                        res_f[mnt_ck] = res_f[mnt_ck][1:]
                    while res_f[mnt_ck][-1].isalnum() == 0:
                        res_f[mnt_ck] = res_f[mnt_ck][:-1]
                if " " in res_f[1]:
                    sp_ck_mnt = res_f[1].split(" ")
                    for cr_mth in sp_ck_mnt:
                        if cr_mth[:3].lower() in list(get_date.keys()):
                            mm = get_date[cr_mth[:3].lower()]
                            dd = doc_dt_slt[0]
                            yy = doc_dt_slt[1]
                            if len(dd) == 1:
                                dd = "0" + str(dd)
                            if len(mm) == 1:
                                mm = "0" + str(mm)
                            if len(yy) == 2:
                                yy = "20" + str(yy)
                            req_date = yy + "-" + mm + "-" + dd
                            date_status = 1
                            break
                elif res_f[1][:3].lower() in list(get_date.keys()):
                    mm = get_date[res_f[1][:3].lower()]
                    dd = doc_dt_slt[0]
                    yy = doc_dt_slt[1]
                    if len(dd) == 1:
                        dd = "0" + str(dd)
                    if len(yy) == 2:
                        yy = "20" + str(yy)
                    if len(mm) == 1:
                        mm = "0" + str(mm)
                    req_date = yy + "-" + mm + "-" + dd
                    date_status = 1
                else:
                    date_status = 0
                    req_date = doc_date
            else:
                date_status = 0
                req_date = doc_date
        elif date_format in ["yyyy mm dd", "yyyy.mm.dd", "yyyy/mm/dd"]:
            doc_dt_slt = re.findall(r"\d+", doc_date)
            if len(doc_dt_slt) == 3:
                yy = doc_dt_slt[0]
                mm = doc_dt_slt[1]
                dd = doc_dt_slt[2]
                if len(dd) == 1:
                    dd = "0" + str(dd)
                if len(yy) == 2:
                    yy = "20" + str(yy)
                if len(mm) == 1:
                    mm = "0" + str(mm)
                req_date = yy + "-" + mm + "-" + dd
                date_status = 1
            elif len(doc_dt_slt) == 2:
                date_res = re.split(
                    r"([-+/]?!S)|([-+]?\d+\.\d+)|([-+/]?\d+)", doc_date.strip()
                )
                res_f = [
                    r.strip() for r in date_res if r is not None and r.strip() != ""
                ]
                while "th" in res_f:
                    res_f.remove("th")
                for mnt_ck in range(len(res_f)):
                    while res_f[mnt_ck][0].isalnum() == 0:
                        res_f[mnt_ck] = res_f[mnt_ck][1:]
                    while res_f[mnt_ck][-1].isalnum() == 0:
                        res_f[mnt_ck] = res_f[mnt_ck][:-1]
                if " " in res_f[1]:
                    sp_ck_mnt = res_f[1].split(" ")
                    for cr_mth in sp_ck_mnt:
                        if cr_mth[:3].lower() in list(get_date.keys()):
                            mm = get_date[cr_mth[:3].lower()]
                            dd = doc_dt_slt[0]
                            yy = doc_dt_slt[1]
                            if len(dd) == 1:
                                dd = "0" + str(dd)
                            if len(mm) == 1:
                                mm = "0" + str(mm)
                            if len(yy) == 2:
                                yy = "20" + str(yy)
                            req_date = yy + "-" + mm + "-" + dd
                            date_status = 1
                elif res_f[1][:3].lower() in list(get_date.keys()):
                    mm = get_date[res_f[1][:3].lower()]
                    yy = doc_dt_slt[0]
                    dd = doc_dt_slt[1]
                    if len(dd) == 1:
                        dd = "0" + str(dd)
                    if len(yy) == 2:
                        yy = "20" + str(yy)
                    if len(mm) == 1:
                        mm = "0" + str(mm)
                    req_date = yy + "-" + mm + "-" + dd
                    date_status = 1
                else:
                    date_status = 0
                    req_date = doc_date
            else:
                date_status = 0
                req_date = doc_date
    except Exception as dt_ck_:
        logger.error(f" {traceback.format_exc()}")
        logger.error(f"line 185 postprocessing: {str(dt_ck_)}")
        # print(str(dt_ck_))
        date_status = 0
        req_date = doc_date
    if date_status == 1:
        try:
            # newDate = datetime.datetime(int(yy), int(mm), int(dd))
            # # TODO: Unused variable
            correctDate = True
        except ValueError:
            correctDate = False
        if not correctDate:
            date_status = 0

    return req_date, date_status


def po_cvn(po_extracted, entityID):

    po_extracted_cln = re.sub(r"\W+", "", po_extracted)

    # po_extracted1 = po_extracted_cln[:11]

    extracted_po_num = "".join(re.split("[^0-9]*", po_extracted_cln))
    extracted_po_num = extracted_po_num.lstrip("0")

    len_po_num = len(extracted_po_num)
    po_status = 0

    po_eid = (
        "select EntityCode from "
        + SCHEMA
        + ".entity where idEntity="
        + str(entityID)
        + ";"
    )
    po_eid_df = pd.read_sql(po_eid, SQLALCHEMY_DATABASE_URL)
    po_df = pd.DataFrame(po_eid_df["EntityCode"])
    po_eid_val = list(po_df["EntityCode"])

    try:
        if len_po_num >= 6:
            formatted_po = po_eid_val[0] + "-PO-" + extracted_po_num

        elif len_po_num < 6 and len_po_num != 0:
            extracted_po_num = extracted_po_num.zfill(6)
            formatted_po = po_eid_val[0] + "-PO-" + extracted_po_num

        elif len_po_num == 0:
            formatted_po = po_extracted
            po_status = 1

    except Exception as po:
        # print("Purchase Order EXCEPTION: ",str(po))
        logger.error(f"Purchase Order EXCEPTION: {str(po)}")
        logger.error(f" {traceback.format_exc()}")

    return formatted_po, po_status


def tb_cln_amt(amt):
    if amt is None:
        amt = "0"
    # cln_amt_sts = 0  # TODO: Unused variable
    amt_cpy = amt
    logger.info(f"cln_amt - amt: {amt}")
    # amt = amt.replace(',','.')
    try:
        if "," in amt:
            if amt[-3] == ",":
                amt = amt[:-3] + "." + amt[-2:]
    except Exception as at:
        logger.info(f"post pro line 247: {str(at)}")
        logger.error(f" {traceback.format_exc()}")
        amt = amt_cpy
    try:
        inl = 1
        fn = 0
        sb_amt = ""
        for amt_sp in amt:
            # print(amt_sp)
            if amt_sp.isdigit():
                sb_amt = sb_amt + amt_sp
                inl = inl * 1
                fn = 1
            elif (inl == 1) and (amt_sp == ".") and fn == 1:
                # print('------')
                sb_amt = sb_amt + amt_sp
                inl = 2
        sb_amt = float(sb_amt)
        sb_amt = round(sb_amt, 2)
        # cln_amt_sts = 1

    except Exception as e:
        logger.error(f" {traceback.format_exc()}")
        sb_amt = amt

        logger.info(f"postpro line 269: {str(e)}")
        sb_amt = str(sb_amt)

    return sb_amt


def getBBox(data):
    try:
        if isinstance(data, list):
            data = data[0]
            # # Extract x, y, width, height
        polygon = data["polygon"]
        x_values = [point["x"] for point in polygon]
        y_values = [point["y"] for point in polygon]
        x = round(min(x_values), 2)
        y = round(min(y_values), 2)
        w = str(round(max(x_values) - x, 2))
        h = str(round(max(y_values) - y, 2))
        x = str(x)
        y = str(y)

        logger.info(f"x: {x}, y: {y}, width: {w}, height: {h}")
    except Exception:
        x = ""
        y = ""
        w = ""
        h = ""
    return {"x": x, "y": y, "w": w, "h": h}


def cln_amt(amt):
    amt_cpy = amt
    if amt is None:
        amt = "0"
    # amt = str(amt).replace(',','.')
    try:
        if "," in amt:
            if amt[-3] == ",":
                amt = amt[:-3] + "." + amt[-2:]
    except Exception as er:
        logger.error(f"Exception in cln_amt: {er}")
        amt = amt_cpy

    if len(amt) > 0 and (amt is not None):
        if len(re.findall(r"\d+\,\d+\d+\.\d+", amt)) > 0:
            cl_amt = re.findall(r"\d+\,\d+\d+\.\d+", amt)[0]
            cl_amt = float(cl_amt.replace(",", ""))
        elif len(re.findall(r"\d+\.\d+", amt)) > 0:
            cl_amt = re.findall(r"\d+\.\d+", amt)[0]
            cl_amt = float(cl_amt)
        elif len(re.findall(r"\d+", amt)) > 0:
            cl_amt = re.findall(r"\d+", amt)[0]
            cl_amt = float(cl_amt)
        else:
            cl_amt = amt
    else:
        cl_amt = amt
    try:
        cl_amt = round(cl_amt, 2)
    except Exception:
        logger.error(traceback.format_exc())
        cl_amt = amt
    return cl_amt


def dataPrep_postprocess_prebuilt(input_data):
    # Function to preprocess the prebuilt data
    all_pg_data = {}
    getData_headerPg = (
        {}
    )  # replace with pg_1['analyzeResult']['documents'][0]['fields']

    for pg_data in input_data:

        pre_pg_data = pg_data["documents"][0]["fields"].copy()

        for tgs in pre_pg_data:
            # print(tgs)

            if tgs not in ("Items"):
                if tgs in getData_headerPg:
                    if (getData_headerPg[tgs]["confidence"]) < (
                        pre_pg_data[tgs]["confidence"]
                    ):
                        getData_headerPg.update({tgs: pre_pg_data[tgs]})
                else:
                    getData_headerPg[tgs] = pre_pg_data[tgs]

    all_pg_data = input_data[0].copy()
    all_pg_data["documents"][0]["fields"] = getData_headerPg

    return all_pg_data


def getFrData_MNF(input_data):

    req_lt_prBlt = [
        "CustomerName",
        "InvoiceDate",
        "InvoiceId",
        "InvoiceTotal",
        "SubTotal",
        "VendorName",
        "VendorAddress",
    ]
    req_lt_prBlt_ln = ["Description", "Quantity", "UnitPrice", "Tax", "Amount"]
    # all_pg_data = {}  # TODO: Unused variable
    # def dataPrep_postprocess(input_data):
    getData_headerPg = (
        {}
    )  # replace with pg_1['analyzeResult']['documents'][0]['fields']
    try:
        for pg_data in input_data:
            # print(pg_data['analyzeResult']['documents'][0]['fields'].keys())
            pre_pg_data = pg_data["documents"][0]["fields"].copy()

            for tgs in pre_pg_data:
                # print(tgs)

                if tgs not in ("Items"):

                    if tgs in getData_headerPg:
                        if (getData_headerPg[tgs]["confidence"]) < (
                            pre_pg_data[tgs]["confidence"]
                        ):
                            getData_headerPg.update({tgs: pre_pg_data[tgs]})
                    else:
                        getData_headerPg[tgs] = pre_pg_data[tgs]
        preBltFrdata = {}
        tmpPrBlt = {}
        preBlt_headerDt = []
        for prTg in getData_headerPg:
            tmpPrBlt = {}
            sbTmpPrblt = {}
            if prTg in getData_headerPg.keys():
                tmpPrBlt["tag"] = prTg
                if "content" in getData_headerPg[prTg]:
                    if prTg in req_lt_prBlt:

                        sbTmpPrblt["value"] = getData_headerPg[prTg]["content"]
                        cfd = float(getData_headerPg[prTg]["confidence"]) * 100

                        sbTmpPrblt["prebuilt_confidence"] = cfd
                        sbTmpPrblt["custom_confidence"] = ""
                        tmpPrBlt["bounding_regions"] = {}

                        if cfd > 90:
                            tmpPrBlt["status"] = 1
                            tmpPrBlt["status_message"] = "Prebuilt confidence: " + str(
                                cfd
                            )
                        else:
                            tmpPrBlt["status"] = 0
                            tmpPrBlt["status_message"] = (
                                "Prebuilt Low confidence: " + str(cfd)
                            )

                        if prTg in [
                            "InvoiceTotal",
                            "SubTotal",
                            "TotalTax",
                            "GST",
                            "PST",
                            "HST",
                        ]:

                            if isinstance(
                                tb_cln_amt(getData_headerPg[prTg]["content"]), float
                            ):
                                sbTmpPrblt["value"] = tb_cln_amt(
                                    getData_headerPg[prTg]["content"]
                                )
                                tmpPrBlt["status"] = 1
                            else:
                                tmpPrBlt["status"] = 0
                                tmpPrBlt["status_message"] = f"Invalid {prTg} Value"
                                sbTmpPrblt["value"] = ""
                        tmpPrBlt["data"] = sbTmpPrblt
                        preBlt_headerDt.append(tmpPrBlt)

        preBltFrdata["header"] = preBlt_headerDt
        PreTabDt = []
        if "Items" in pre_pg_data:
            if "value" in pre_pg_data["Items"]:
                for prBtLn in range(len(pre_pg_data["Items"]["value"])):
                    tmpRwLt = []
                    if "value" in pre_pg_data["Items"]["value"][prBtLn]:
                        # row values:

                        for vlObt in pre_pg_data["Items"]["value"][prBtLn]["value"]:
                            if vlObt in req_lt_prBlt_ln:
                                if (
                                    "content"
                                    in pre_pg_data["Items"]["value"][prBtLn]["value"][
                                        vlObt
                                    ]
                                ):
                                    rwVls = {
                                        "tag": vlObt,
                                        "data": str(
                                            pre_pg_data["Items"]["value"][prBtLn][
                                                "value"
                                            ][vlObt]["content"]
                                        ),
                                        "bounding_regions": {},
                                        "row_count": prBtLn + 1,
                                    }
                                    tmpRwLt.append(rwVls)
                                    rwVls = {}
                    PreTabDt.append(tmpRwLt)

        preBltFrdata["tab"] = PreTabDt
        preBltFrdata["overall_status"] = 0
        preBltFrdata["prebuilt_header"] = getData_headerPg
        preBltFrdata_status = 1
    except Exception as e:
        logger.info(f"preBltFrdat Exception line 432: {str(e)}")
        logger.error(f" {traceback.format_exc()}")
        preBltFrdata_status = 0

    return preBltFrdata, preBltFrdata_status


def tab_to_dict(new_invoLineData_df, itemCode, typ=""):
    invo_NW_itemDict = {}
    des = ""
    for itmId in list(new_invoLineData_df[itemCode].unique()):
        tmpdf = new_invoLineData_df[new_invoLineData_df[itemCode] == itmId].reset_index(
            drop=True
        )
        tmpdict = {}
        for ch in range(0, len(tmpdf)):
            val = tmpdf["Value"][ch]
            tg_nm = tmpdf["TagName"][ch]
            if tg_nm in ["Description", "Quantity"]:
                if tg_nm == "Description":
                    des = val
                tmpdict[tg_nm] = val
        if typ == "grn":
            invo_NW_itemDict[itmId] = tmpdict
        else:
            invo_NW_itemDict[itmId + "__" + des] = tmpdict
    return invo_NW_itemDict


def dataPrep_postprocess_cust(input_data):

    all_pg_data = {}
    # def dataPrep_postprocess(input_data):
    getData_headerPg = (
        {}
    )  # replace with pg_1['analyzeResult']['documents'][0]['fields']
    getData_TabPg = (
        []
    )  # pg_1['analyzeResult']['documents'][0]['fields']['tab_1']['value']
    cnt = 0
    for pg_data in input_data:
        cust_pg_data = pg_data["documents"][0]["fields"].copy()
        if "tab_1" in pg_data["documents"][0]["fields"].keys():
            cust_tab_pg_data = (
                pg_data["documents"][0]["fields"]["tab_1"]["value"].copy()
                if "value" in pg_data["documents"][0]["fields"]["tab_1"]
                else []
            )

        for pg_rw in cust_tab_pg_data:
            cnt = cnt + 1
            getData_TabPg.append(pg_rw)

        for tgs in cust_pg_data:
            if tgs not in ("tab_1", "tab_2", "tab_3", "tab_3", "Items"):
                if tgs in getData_headerPg.keys():
                    if "content" in cust_pg_data[tgs]:
                        if (getData_headerPg[tgs]["confidence"]) < (
                            cust_pg_data[tgs]["confidence"]
                        ):
                            getData_headerPg.update({tgs: cust_pg_data[tgs]})
                else:
                    getData_headerPg[tgs] = cust_pg_data[tgs]

    all_pg_data = input_data[0].copy()
    all_pg_data["documents"][0]["fields"] = getData_headerPg
    # tb_dict = {'tab_1': {'type': 'array', 'value': getData_TabPg}}
    # all_pg_data['documents'][0]['fields']['tab_1'] =}
    # all_pg_data['documents'][0]['fields']
    all_pg_data["documents"][0]["fields"]["tab_1"] = {
        "type": "array",
        "value": getData_TabPg,
    }
    # ['tab_1']['value']
    return all_pg_data


def postpro(
    cst_data_,
    pre_data_,
    date_format,
    invo_model_id,
    SQLALCHEMY_DATABASE_URL,
    entityID,
    vendorAccountID,
    filename,
    db,
    sender,
    metaVendorName,
    metaVendorAdd,
):
    global qty_rw_status, amt_withTax_rw_status  # TODO dont use global variable
    global vatAmt_rw_status, utprice_rw_status  # TODO dont use global variable
    global vatAmt_rw, amt_withTax_rw, discount_rw_status  # TODO no global variable
    duplicate_status = 1
    default_qty_ut = 0
    tab_cal_unitprice = 0
    subtotal_Cal = 0
    tab_cal_unitprice_AmtExcTax = 0
    cust_oly = 0
    missing_rw_tab = []
    subtotal_rw = ""
    totaldiscount_rw = ""
    totalTax_rw = ""
    invoiceTotal_rw = ""
    skp_tab_mand_ck = 0
    doc_VendorName = ""
    doc_VendorAddress = ""

    try:

        cst_data = dataPrep_postprocess_cust(cst_data_)
        pre_data = dataPrep_postprocess_prebuilt(pre_data_)

        mandatorylinetags = (
            db.query(model.FRMetaData.mandatorylinetags)
            .filter(model.FRMetaData.idInvoiceModel == invo_model_id)
            .scalar()
        )
        mandatoryheadertags = (
            db.query(model.FRMetaData.mandatoryheadertags)
            .filter(model.FRMetaData.idInvoiceModel == invo_model_id)
            .scalar()
        )
        # DateFormat = (
        #     db.query(model.FRMetaData.DateFormat)
        #     .filter(model.FRMetaData.idInvoiceModel == invo_model_id)
        #     .scalar()
        # )  # TODO: Unused variable

        mandatory_header = mandatoryheadertags.split(",")

        mandatory_tab_col = mandatorylinetags.split(",")
        field_threshold = 0.7

        cst_tmp_dict = {}
        cust_header = []
        for cst_hd in cst_data["documents"][0]["fields"]:
            cust_header.append(cst_hd)
            if "content" in cst_data["documents"][0]["fields"][cst_hd]:
                if (
                    "bounding_regions"
                    in cst_data["documents"][0]["fields"][cst_hd].keys()
                ):
                    try:
                        bounding_regions = getBBox(
                            cst_data["documents"][0]["fields"][cst_hd][
                                "bounding_regions"
                            ]
                        )

                    except Exception:
                        logger.error(traceback.format_exc())
                        bx = {}

                        bx["x"] = ""
                        bx["y"] = ""
                        bx["w"] = ""
                        bx["h"] = ""

                        bounding_regions = bx
                    cst_tmp_dict[cst_hd] = {
                        "content": cst_data["documents"][0]["fields"][cst_hd][
                            "content"
                        ],
                        "confidence": cst_data["documents"][0]["fields"][cst_hd][
                            "confidence"
                        ],
                        "bounding_regions": bounding_regions,
                    }
                else:
                    cst_tmp_dict[cst_hd] = {
                        "content": cst_data["documents"][0]["fields"][cst_hd][
                            "content"
                        ],
                        "confidence": 0,
                        "bounding_regions": "",
                    }

        pre_tmp_dict = {}
        pre_headers = []
        check_pre_hd = []

        check_pre_hd = {}
        for pre_hd in pre_data["documents"][0]["fields"]:

            if "content" in pre_data["documents"][0]["fields"][pre_hd]:
                pre_tmp_dict[pre_hd] = {
                    "content": pre_data["documents"][0]["fields"][pre_hd]["content"],
                    "confidence": (
                        pre_data["documents"][0]["fields"][pre_hd]["confidence"]
                        if "confidence" in pre_data["documents"][0]["fields"][pre_hd]
                        else "0"
                    ),
                    "bounding_regions": (
                        pre_data["documents"][0]["fields"][pre_hd]["bounding_regions"]
                        if "bounding_regions"
                        in pre_data["documents"][0]["fields"][pre_hd]
                        else [0, 0, 0, 0, 0, 0]
                    ),
                }

        add_cust_set = set(cust_header) - set(check_pre_hd)
        add_cust = list(add_cust_set)

        for add_hd in add_cust:
            add_custom = {}
            add_custom = {
                "tag": add_hd,
                "data": {
                    "value": "None",
                    "prebuilt_confidence": "0",
                    "custom_confidence": "0",
                },
                "bounding_regions": {"x": "0", "y": "0", "w": "0", "h": "0"},
                "status": "0",
                "status_message": "Prebuilt failed to extract",
            }
            pre_headers.append(add_custom)

        pre_dict = {}
        cst_dict = cst_tmp_dict

        sm_tag = set(cst_dict.keys()).intersection(set(pre_dict.keys()))
        cst_tag = set(cst_dict.keys()).difference(sm_tag)
        fr_headers = []
        ovrll_conf_ck = 1
        field_threshold = 0.7
        status_message = ""
        for hd_tags in sm_tag:
            tmp_fr_headers = {}

            pre_conf = float(pre_dict[hd_tags]["confidence"])

            cst_conf = float(cst_dict[hd_tags]["confidence"])

            if max(cst_conf, pre_conf) >= field_threshold:
                tag_status = 1

            else:
                tag_status = 0
                ovrll_conf_ck = ovrll_conf_ck * 0

                try:
                    status_message = (
                        "Low confidence: " + str(max(cst_conf, pre_conf) * 100) + "%."
                    )
                except Exception as et:
                    logger.error(
                        "Exception in postprocessing:"
                        + f"(Low confidence,Please review.) {str(et)}"
                    )
                    status_message = "Low confidence,Please review."

            if cust_oly == 1:
                if "content" in cst_dict[hd_tags]:
                    tag_val = cst_dict[hd_tags]["content"]
                if "bounding_regions" in cst_dict[hd_tags]:
                    bounding_bx = cst_dict[hd_tags]["bounding_regions"]
            else:

                if cst_conf < pre_conf:
                    if hd_tags == "VendorName":
                        if "content" in cst_dict[hd_tags]:
                            tag_val = cst_dict[hd_tags]["content"]
                        if "bounding_regions" in cst_dict[hd_tags]:
                            bounding_bx = cst_dict[hd_tags]["bounding_regions"]
                    if (cst_conf + 0.2) > pre_conf:
                        if "content" in cst_dict[hd_tags]:
                            tag_val = cst_dict[hd_tags]["content"]
                        if "bounding_regions" in cst_dict[hd_tags]:
                            bounding_bx = cst_dict[hd_tags]["bounding_regions"]
                    else:
                        if "content" in pre_dict[hd_tags]:
                            tag_val = pre_dict[hd_tags]["content"]
                        if "bounding_regions" in pre_dict[hd_tags]:
                            bounding_bx = pre_dict[hd_tags]["bounding_regions"]
                elif pre_conf < cst_conf:
                    if "content" in cst_dict[hd_tags]:
                        tag_val = cst_dict[hd_tags]["content"]
                    if "bounding_regions" in cst_dict[hd_tags]:
                        bounding_regions = cst_dict[hd_tags]["bounding_regions"]
                elif pre_conf == cst_conf:
                    if "content" in cst_dict[hd_tags]:
                        tag_val = cst_dict[hd_tags]["content"]
                    if "bounding_regions" in cst_dict[hd_tags]:
                        bounding_bx = cst_dict[hd_tags]["bounding_regions"]
                if (pre_conf < cst_conf) and (abs(pre_conf - cst_conf) > 0.3):
                    if (
                        pre_conf == "" and cst_conf < field_threshold
                    ) or cst_conf < field_threshold:
                        tag_status = 0
                        ovrll_conf_ck = ovrll_conf_ck * 0
                        status_message = (
                            "Low Confidence Detected: " + str(cst_conf * 100) + "%."
                        )
                    else:
                        status_message = "Low Confidence Detected"

            tmp_fr_headers["tag"] = hd_tags
            tmp_fr_headers["data"] = {
                "value": tag_val,
                "prebuilt_confidence": str(pre_conf),
                "custom_confidence": str(cst_conf),
            }
            try:
                bx = getBBox(bounding_bx)
            except Exception:
                logger.error(traceback.format_exc())
                bx = {"x": "", "y": "", "w": "", "h": ""}
            tmp_fr_headers["bounding_regions"] = bx

            tmp_fr_headers["status"] = tag_status
            tmp_fr_headers["status_message"] = status_message
            fr_headers.append(tmp_fr_headers)
        for ct_tag in cst_tag:
            tmp_fr_headers = {}
            if ct_tag != "Items":
                if "content" in cst_dict[ct_tag]:
                    if "confidence" in cst_dict[ct_tag]:
                        # print("cst_dict[ct_tag]: ",cst_dict[ct_tag])
                        cst_conf = float(cst_dict[ct_tag]["confidence"])
                    tag_val = cst_dict[ct_tag]["content"]
                    if "bounding_regions" in cst_dict[ct_tag]:
                        bounding_bx = cst_dict[ct_tag]["bounding_regions"]
                    if cst_conf >= field_threshold:
                        tag_status = 1
                        status_message = "No OCR Issues Detected"
                    else:
                        tag_status = 0
                        status_message = (
                            "Low Confidence Detected:" + str(cst_conf * 100) + "%."
                        )
                        ovrll_conf_ck = ovrll_conf_ck * 0
                    if ct_tag in ["InvoiceTotal", "SubTotal", "TotalTax"]:
                        if isinstance(tb_cln_amt(cst_dict[ct_tag]["content"]), float):
                            tag_status = 1
                        else:
                            tag_status = 0
                            status_message = f"Invalid {ct_tag} Value"
                    tmp_fr_headers["tag"] = ct_tag
                    tmp_fr_headers["data"] = {
                        "value": tag_val,
                        "prebuilt_confidence": "",
                        "custom_confidence": str(cst_conf),
                    }

                    if bounding_bx != "":
                        bx = getBBox(bounding_bx)
                        tmp_fr_headers["bounding_regions"] = bx
                        tmp_fr_headers["status"] = tag_status
                        tmp_fr_headers["status_message"] = status_message
                    else:
                        # tag_status = 0
                        bx = {"x": "", "y": "", "w": "", "h": ""}
                        tmp_fr_headers["bounding_regions"] = bx
                        tmp_fr_headers["status"] = tag_status
                        tmp_fr_headers["status_message"] = status_message
                fr_headers.append(tmp_fr_headers)
        overall_status = ovrll_conf_ck

        # cst_data['analyzeResult']['documents'][0]
        fields = cst_data["documents"][0]["fields"]
        # tab data:
        tabs = [
            tb
            for tb in list(cst_data["documents"][0]["fields"].keys())
            if tb.startswith("tab_")
        ]
        # print("tabs: ", tabs)
        itm_list = []
        # fields = cst_data['analyzeResult']['documents'][0]['fields']
        ignore_tags = ["SerialNo", "Item"]
        for tbs in tabs:
            if "value" in fields[tbs]:
                for itm_no in range(len(fields[tbs]["value"])):
                    tmp_dict = {}
                    tmp_list = []

                    present_tab_header = []
                    for ky in fields[tbs]["value"][itm_no]["value"]:

                        if ky not in ignore_tags:

                            if fields[tbs]["value"][itm_no]["value"][ky] is None:
                                tmp_dict["tag"] = ky
                                if fields[tbs]["value"][itm_no]["value"][ky] != "":
                                    tmp_dict["data"] = ""
                                    bx = {}
                                    if (
                                        "bounding_regions"
                                        in fields[tbs]["value"][itm_no]["value"][ky]
                                    ):
                                        bo_bx = fields[tbs]["value"][itm_no]["value"][
                                            ky
                                        ]["bounding_regions"]
                                    else:
                                        bo_bx = [0, 0, 0, 0, 0, 0]
                                    bx = getBBox(bo_bx)

                                    tmp_dict["bounding_regions"] = bx
                                    tmp_list.append(tmp_dict)
                                    tmp_dict = {}
                                else:
                                    tmp_dict["data"] = ""
                                    tmp_dict["bounding_regions"] = {
                                        "x": "",
                                        "y": "",
                                        "w": "",
                                        "h": "",
                                    }

                            else:
                                tmp_dict["tag"] = ky
                                if fields[tbs]["value"][itm_no]["value"][ky] != "":
                                    tmp_dict["data"] = (
                                        fields[tbs]["value"][itm_no]["value"][ky][
                                            "content"
                                        ]
                                        if "content"
                                        in fields[tbs]["value"][itm_no]["value"][ky]
                                        else ""
                                    )
                                    bx = {}
                                    try:
                                        bo_bx = fields[tbs]["value"][itm_no]["value"][
                                            ky
                                        ]["bounding_regions"]
                                    except KeyError:
                                        bo_bx = [0, 0, 0, 0, 0, 0]
                                    try:

                                        if len(bo_bx) > 0 and isinstance(
                                            bo_bx[0], dict
                                        ):
                                            bx = getBBox(bo_bx)
                                        else:
                                            bx = {"x": "", "y": "", "w": "", "h": ""}

                                    except Exception:
                                        logger.error(traceback.format_exc())
                                        bo_bx = [0, 0, 0, 0, 0, 0]
                                        bx = {"x": "", "y": "", "w": "", "h": ""}

                                    tmp_dict["bounding_regions"] = bx

                                    if tmp_dict["tag"] in [
                                        "AmountExcTax",
                                        "UnitPrice",
                                        "Amount",
                                        "Quantity",
                                    ]:
                                        # call
                                        cl_dt = tb_cln_amt(tmp_dict["data"])
                                        tmp_dict["data"] = str(cl_dt)
                                        if isinstance(cl_dt, str):
                                            tmp_dict["status"] = 0
                                            tmp_dict["status_message"] = (
                                                "Low Confidence "
                                                + "or Missing Value Detected"
                                            )

                                    if default_qty_ut == 1:
                                        if tmp_dict["tag"] in ["Quantity", "UnitPrice"]:
                                            tmp_dict["data"] = 1
                                            bx["x"] = "0"
                                            bx["y"] = "0"
                                            bx["w"] = "0"
                                            bx["h"] = "0"
                                            tmp_dict["bounding_regions"] = bx
                                            # tmp_list.append(tmp_dict)
                                            # tmp_dict = {}
                                    present_tab_header.append(tmp_dict["tag"])
                                    tmp_list.append(tmp_dict)
                                    tmp_dict = {}

                                else:
                                    tmp_dict["data"] = ""
                                    tmp_dict["bounding_regions"] = None

                    if tab_cal_unitprice_AmtExcTax == 1:
                        if "AmountExcTax" not in present_tab_header:
                            tmp_dict["tag"] = "AmountExcTax"

                            tmp_dict["data"] = ""
                            bx["x"] = "0"
                            bx["y"] = "0"
                            bx["w"] = "0"
                            bx["h"] = "0"
                            tmp_dict["bounding_regions"] = bx
                            tmp_dict["status"] = 0
                            tmp_dict["status_message"] = "Mandatory Value Missing"

                            tmp_list.append(tmp_dict)
                            present_tab_header.append("Quantity")
                            tmp_dict = {}

                    if default_qty_ut == 1:
                        if "Quantity" not in present_tab_header:
                            tmp_dict["tag"] = "Quantity"

                            # if ky in ['Quantity', 'UnitPrice']:
                            #     if default_qty_ut == 1:
                            tmp_dict["data"] = 1
                            bx["x"] = "0"
                            bx["y"] = "0"
                            bx["w"] = "0"
                            bx["h"] = "0"
                            tmp_dict["bounding_regions"] = bx
                            tmp_list.append(tmp_dict)
                            present_tab_header.append("Quantity")
                            tmp_dict = {}
                        if "UnitPrice" not in present_tab_header:
                            tmp_dict["tag"] = "UnitPrice"
                            # if ky in ['Quantity', 'UnitPrice']:
                            #     if default_qty_ut == 1:
                            tmp_dict["data"] = 1
                            bx["x"] = "0"
                            bx["y"] = "0"
                            bx["w"] = "0"
                            bx["h"] = "0"
                            tmp_dict["bounding_regions"] = bx
                            tmp_list.append(tmp_dict)
                            tmp_dict = {}
                            present_tab_header.append("UnitPrice")

                    itm_list.append(tmp_list)
        try:
            for rw_ck_1 in range(0, len(itm_list)):
                # missing_tab_val = []  # TODO: Unused variable
                prst_rw_val = []
                # rw_ck_1 = 0
                utprice_rw = ""
                amxExtx_rw = ""
                discount_rw = ""
                qty_rw = ""
                for ech_tg in range(0, len(itm_list[rw_ck_1])):
                    # print(itm_list[rw_ck_1][ech_tg]['tag'])
                    if itm_list[rw_ck_1][ech_tg]["tag"] in (
                        "Quantity",
                        "Discount",
                        "UnitPrice",
                        "AmountExcTax",
                    ):
                        try:
                            qty_ck_cl = cln_amt(itm_list[rw_ck_1][ech_tg]["data"])
                            if isinstance(qty_ck_cl, str):
                                itm_list[rw_ck_1][ech_tg]["status"] = 0
                                itm_list[rw_ck_1][ech_tg][
                                    "status_message"
                                ] = "Low Confidence Detected"
                            elif isinstance(qty_ck_cl, float):
                                itm_list[rw_ck_1][ech_tg]["data"] = str(qty_ck_cl)
                            else:
                                itm_list[rw_ck_1][ech_tg]["status"] = 0
                                itm_list[rw_ck_1][ech_tg][
                                    "status_message"
                                ] = "Low Confidence Detected"

                        except Exception as qt:
                            itm_list[rw_ck_1][ech_tg]["status"] = 0
                            itm_list[rw_ck_1][ech_tg][
                                "status_message"
                            ] = "Low Confidence Detected"
                            logger.error(f"exception in postprocess cln: { str(qt)}")

                    if itm_list[rw_ck_1][ech_tg]["tag"] in mandatory_tab_col:
                        if itm_list[rw_ck_1][ech_tg]["data"] == "":
                            itm_list[rw_ck_1][ech_tg]["status"] = 0
                            itm_list[rw_ck_1][ech_tg][
                                "status_message"
                            ] = "Mandatory value missing"
                for ech_tg_1 in itm_list[rw_ck_1]:
                    prst_rw_val.append(ech_tg_1["tag"])
                    if tab_cal_unitprice == 1:
                        if ech_tg_1["tag"] == "Quantity":
                            qty_rw = ech_tg_1["data"]
                        if ech_tg_1["tag"] == "Discount":
                            discount_rw = ech_tg_1["data"]
                        if ech_tg_1["tag"] == "UnitPrice":
                            utprice_rw = ech_tg_1["data"]
                        if ech_tg_1["tag"] == "AmountExcTax":
                            amxExtx_rw = ech_tg_1["data"]
                    if tab_cal_unitprice_AmtExcTax == 1:
                        if ech_tg_1["tag"] == "Quantity":
                            qty_rw = ech_tg_1["data"]
                            if "status" in ech_tg_1:
                                qty_rw_status = ech_tg_1["status"]
                            else:
                                cln_qty_ck = tb_cln_amt(qty_rw)
                                if isinstance(cln_qty_ck, float):
                                    qty_rw_status = 1
                                else:
                                    qty_rw_status = 0

                        if ech_tg_1["tag"] == "Discount":
                            discount_rw = ech_tg_1["data"]
                            if "status" in ech_tg_1:
                                discount_rw_status = ech_tg_1["status"]
                            else:
                                cln_dis_cmt = tb_cln_amt(discount_rw)
                                if isinstance(cln_dis_cmt, float):
                                    discount_rw_status = 1
                                else:
                                    discount_rw_status = 0

                        if ech_tg_1["tag"] == "UnitPrice":
                            utprice_rw = ech_tg_1["data"]
                            if "status" in ech_tg_1:
                                utprice_rw_status = ech_tg_1["status"]
                            else:
                                qt_cln_val = tb_cln_amt(utprice_rw)
                                if isinstance(qt_cln_val, float):
                                    utprice_rw_status = 1
                                else:
                                    utprice_rw_status = 0

                        if ech_tg_1["tag"] == "Amount":
                            amt_withTax_rw = ech_tg_1["data"]
                            if "status" in ech_tg_1:
                                amt_withTax_rw_status = ech_tg_1["status"]
                            else:
                                cl_val = tb_cln_amt(amt_withTax_rw)
                                if isinstance(cl_val, float):
                                    amt_withTax_rw_status = 1
                                else:
                                    amt_withTax_rw_status = 0
                        if ech_tg_1["tag"] == "Tax":
                            vatAmt_rw = ech_tg_1["data"]
                            if "status" in ech_tg_1:
                                amt_withTax_rw_status = ech_tg_1["status"]
                            else:
                                vtcln_amt = tb_cln_amt(vatAmt_rw)
                                if isinstance(vtcln_amt, float):
                                    vatAmt_rw_status = 1
                                else:
                                    vatAmt_rw_status = 0

                if tab_cal_unitprice_AmtExcTax == 1:
                    if (
                        (qty_rw != "")
                        and (discount_rw != "")
                        and (utprice_rw != "")
                        and (amt_withTax_rw != "")
                        and (vatAmt_rw != "")
                    ):
                        if (
                            (qty_rw_status == 1)
                            and (discount_rw_status == 1)
                            and (utprice_rw_status == 1)
                            and (amt_withTax_rw_status == 1)
                            and (vatAmt_rw_status == 1)
                        ):
                            qty_rw = cln_amt(qty_rw)
                            utprice_rw = cln_amt(utprice_rw)
                            amt_withTax_rw = cln_amt(amt_withTax_rw)
                            discount_rw = cln_amt(discount_rw)
                            vatAmt_rw = cln_amt(vatAmt_rw)
                            amt_excTax_cal = amt_withTax_rw - vatAmt_rw
                            try:
                                cal_utprice_rw = utprice_rw - (discount_rw / qty_rw)
                            except Exception as cl:
                                logger.error(
                                    "exception in postprocess cln for "
                                    + "(cal_utprice_rw = "
                                    + "utprice_rw - "
                                    + f"(discount_rw / qty_rw)): { str(cl)}"
                                )
                                cal_utprice_rw = ""

                            try:
                                cal_amtExTx_PE = amt_excTax_cal / qty_rw
                            except Exception as cl:
                                logger.error(
                                    "exception in postprocess cln for "
                                    + "(cal_amtExTx_PE = "
                                    + f" amt_excTax_cal / qty_rw): { str(cl)}"
                                )
                                cal_amtExTx_PE = ""

                            if cal_utprice_rw == (cal_amtExTx_PE):
                                for ech_tg_4 in range(0, len(itm_list[rw_ck_1])):
                                    if (
                                        itm_list[rw_ck_1][ech_tg_4]["tag"]
                                        == "UnitPrice"
                                    ):
                                        itm_list[rw_ck_1][ech_tg_4][
                                            "data"
                                        ] = cal_utprice_rw
                                    if (
                                        itm_list[rw_ck_1][ech_tg_4]["tag"]
                                        == "AmountExcTax"
                                    ):
                                        itm_list[rw_ck_1][ech_tg_4]["status"] = 1
                                        itm_list[rw_ck_1][ech_tg_4][
                                            "data"
                                        ] = amt_excTax_cal
                                        itm_list[rw_ck_1][ech_tg_4][
                                            "status_message"
                                        ] = "Calculated value"

                            else:
                                for ech_tg_2 in range(0, len(itm_list[rw_ck_1])):
                                    if (
                                        itm_list[rw_ck_1][ech_tg_2]["tag"]
                                        == "UnitPrice"
                                    ):
                                        itm_list[rw_ck_1][ech_tg_2]["status"] = 0
                                        itm_list[rw_ck_1][ech_tg_2][
                                            "status_message"
                                        ] = "Unit Price Calculation "
                                        +"with Discount Failed"
                                    if (
                                        itm_list[rw_ck_1][ech_tg_2]["tag"]
                                        == "AmountExcTax"
                                    ):
                                        itm_list[rw_ck_1][ech_tg_2]["data"] = ""
                                        itm_list[rw_ck_1][ech_tg_2]["status"] = 0
                                        itm_list[rw_ck_1][ech_tg_2][
                                            "status_message"
                                        ] = "AmountExcTax calculation "
                                        +"with discount failed"
                        else:

                            for ech_tg_2 in range(0, len(itm_list[rw_ck_1])):
                                if itm_list[rw_ck_1][ech_tg_2]["tag"] == "UnitPrice":
                                    itm_list[rw_ck_1][ech_tg_2]["status"] = 0
                                    itm_list[rw_ck_1][ech_tg_2][
                                        "status_message"
                                    ] = "Unitprice calculation with discount failed"
                                if itm_list[rw_ck_1][ech_tg_2]["tag"] == "AmountExcTax":
                                    itm_list[rw_ck_1][ech_tg_2]["data"] = ""
                                    itm_list[rw_ck_1][ech_tg_2]["status"] = 0
                                    itm_list[rw_ck_1][ech_tg_2][
                                        "status_message"
                                    ] = "AmountExcTax calculation with discount failed"
                    else:
                        for ech_tg_2 in range(0, len(itm_list[rw_ck_1])):
                            if itm_list[rw_ck_1][ech_tg_2]["tag"] == "UnitPrice":
                                itm_list[rw_ck_1][ech_tg_2]["status"] = 0
                                itm_list[rw_ck_1][ech_tg_2][
                                    "status_message"
                                ] = "Unitprice calculation with discount failed"
                            if itm_list[rw_ck_1][ech_tg_2]["tag"] == "AmountExcTax":
                                itm_list[rw_ck_1][ech_tg_2]["status"] = 0
                                itm_list[rw_ck_1][ech_tg_2][
                                    "status_message"
                                ] = "AmountExcTax calculation with discount failed"

                if tab_cal_unitprice == 1:
                    if (
                        (qty_rw != "")
                        and (discount_rw != "")
                        and (utprice_rw != "")
                        and (amxExtx_rw != "")
                    ):
                        qty_rw = cln_amt(qty_rw)
                        utprice_rw = cln_amt(utprice_rw)
                        amxExtx_rw = cln_amt(amxExtx_rw)
                        discount_rw = cln_amt(discount_rw)
                        try:
                            cal_utprice_rw = utprice_rw - (discount_rw / qty_rw)
                        except Exception as cl_vl:
                            logger.error(f"exception in postprocess cln: { str(cl_vl)}")
                            cal_utprice_rw = ""

                        try:
                            cal_amtExTx_PE = amxExtx_rw / qty_rw
                        except Exception as cl_vl:
                            logger.error(f"exception in postprocess cln: { str(cl_vl)}")
                            cal_amtExTx_PE = ""

                        if (
                            cal_utprice_rw == (cal_amtExTx_PE)
                            and (cal_amtExTx_PE != "")
                            and (cal_utprice_rw != "")
                        ):
                            for ech_tg_4 in range(0, len(itm_list[rw_ck_1])):
                                if itm_list[rw_ck_1][ech_tg_4]["tag"] == "UnitPrice":
                                    itm_list[rw_ck_1][ech_tg_4]["data"] = cal_utprice_rw
                        else:
                            for ech_tg_2 in range(0, len(itm_list[rw_ck_1])):
                                if itm_list[rw_ck_1][ech_tg_2]["tag"] == "UnitPrice":
                                    itm_list[rw_ck_1][ech_tg_2]["status"] = 0
                                    itm_list[rw_ck_1][ech_tg_2][
                                        "status_message"
                                    ] = "Unitprice calculation with discount failed"
                    else:
                        for ech_tg_3 in range(0, len(itm_list[rw_ck_1])):
                            if itm_list[rw_ck_1][ech_tg_3]["tag"] == "UnitPrice":
                                itm_list[rw_ck_1][ech_tg_3]["status"] = 0
                                itm_list[rw_ck_1][ech_tg_3][
                                    "status_message"
                                ] = "Unitprice calculation with discount failed"
                if skp_tab_mand_ck == 1:
                    missing_rw_tab = []
                else:
                    if not set(mandatory_tab_col).issubset(set(prst_rw_val)):
                        missing_rw_tab = list(set(mandatory_tab_col) - set(prst_rw_val))

                    for mis_rw_val in missing_rw_tab:
                        itm_list[rw_ck_1][ech_tg]["tag"] = mis_rw_val
                        itm_list[rw_ck_1][ech_tg]["data"] = ""
                        itm_list[rw_ck_1][ech_tg]["status"] = 0
                        itm_list[rw_ck_1][ech_tg][
                            "status_message"
                        ] = "Mandatory value not detected"
                        itm_list[rw_ck_1][ech_tg]["bounding_regions"] = {
                            "x": "",
                            "y": "",
                            "w": "",
                            "h": "",
                        }
        except Exception as E:
            msg_line = str(E)
            logger.error(f"exception frm new upate on postprocessing: {msg_line}")

        fr_data = {
            "header": fr_headers,
            "tab": itm_list,
            "overall_status": overall_status,
            "prebuilt_header": pre_headers,
        }
        # print("fr_data", fr_data)
        postprocess_status = 1
        postprocess_msg = "success"
        # posted_status = 1  # TODO: Unused variable
        dt = fr_data
        get_nm_trn_qry = (
            'SELECT "TRNNumber","VendorName" FROM '
            + SCHEMA
            + '.vendor where "idVendor" in(SELECT "vendorID" FROM '
            + SCHEMA
            + '.vendoraccount where "idVendorAccount" in '
            + '(SELECT "idVendorAccount" FROM '
            + SCHEMA
            + '.documentmodel WHERE "idDocumentModel"='
            + str(invo_model_id)
            + "));"
        )
        vdr_nm_trn_df = pd.read_sql(get_nm_trn_qry, SQLALCHEMY_DATABASE_URL)
        # vdr_trn_df = pd.DataFrame(vdr_nm_trn_df["TRNNumber"])
        vdr_name_df = pd.DataFrame(vdr_nm_trn_df["VendorName"])

        name_val = (
            list(vdr_name_df["VendorName"])[0]
            if "VendorName" in vdr_name_df and len(list(vdr_name_df["VendorName"])) > 0
            else ""
        )

        for tg in range(len(dt["header"])):
            if dt["header"][tg]["tag"] == "InvoiceId":
                doc_invID = dt["header"][tg]["data"]["value"]
                if len(doc_invID) > 1:
                    while doc_invID[0].isalnum() == 0:
                        doc_invID = doc_invID[1:]
                    while doc_invID[-1].isalnum() == 0:
                        doc_invID = doc_invID[:-1]
                dt["header"][tg]["data"]["value"] = doc_invID
                vendor = model.Vendor
                vendor_account = model.VendorAccount
                document = model.Document

                # Join Vendor and VendorAccount tables on vendorID
                vendor_vendor_account_join = join(
                    vendor, vendor_account, vendor.idVendor == vendor_account.vendorID
                )

                # Construct the final query
                query = (
                    db.query(document.documentStatusID)
                    .join(
                        vendor_vendor_account_join,
                        document.vendorAccountID == vendor_account.idVendorAccount,
                    )
                    .filter(
                        document.docheaderID == str(doc_invID),
                        document.idDocumentType == 3,
                        vendor.VendorName == name_val,
                    )
                    .all()
                )

                if len(query) > 0:
                    for d in query:
                        if d[0] not in [10, 0]:
                            duplicate_status = 0
                            break
                        elif d[0] in [7, 14]:
                            # posted_status = 0  # TODO: Unused variable
                            break

            if dt["header"][tg]["tag"] == "InvoiceDate":
                invo_date = dt["header"][tg]["data"]["value"]
                req_date, date_status = date_cnv(invo_date, date_format)
                if date_status == 1:
                    dt["header"][tg]["data"]["value"] = req_date
                    dt["header"][tg]["status"] = 1
                else:
                    dt["header"][tg]["data"]["value"] = req_date
                    dt["header"][tg]["status"] = 0
                    dt["header"][tg]["status_message"] = "Invalid Date formate"

            # if dt["header"][tg]["tag"] == "TRN":
            #     doc_trn = dt["header"][tg]["data"]["value"]
            #     try:
            #         doc_trn_nw = ""
            #         for trn_sp in doc_trn.split(" "):
            #             # print(trn_sp)
            #             sb_tn = ""
            #             for i_ in trn_sp:
            #                 if i_.isdigit():
            #                     sb_tn = sb_tn + i_
            #             if len(sb_tn) > 0:
            #                 doc_trn_nw = doc_trn_nw + " " + sb_tn

            #         ocr_trn = max(
            #             (re.findall(r"\b\d+\b", doc_trn_nw)), default=0, key=len
            #         )
            #         dt["header"][tg]["data"]["value"] = ocr_trn
            #         # TRN Match:
            #         get_trn_qry = (
            #             'SELECT "TRNNumber" FROM '
            #             + SCHEMA
            #             + '.vendor where "idVendor" in(SELECT "vendorID" FROM '
            #             + SCHEMA
            #             + '.vendoraccount where "idVendorAccount" in '
            #             + '(SELECT "idVendorAccount" FROM '
            #             + SCHEMA
            #             + '.documentmodel WHERE "idDocumentModel"='
            #             + str(invo_model_id)
            #             + "));"
            #         )
            #         vdr_trn_df = pd.read_sql(get_trn_qry, SQLALCHEMY_DATABASE_URL)
            #         trn_val = (
            #             list(vdr_trn_df["TRNNumber"])[0]
            #             if "TRNNumber" in vdr_trn_df
            #             and len(list(vdr_trn_df["TRNNumber"])) > 0
            #             else ""
            #         )
            #         if trn_val == "":
            #             dt["header"][tg][
            #                 "status_message"
            #             ] = "TRN is missing from Vendor metadata."
            #             dt["header"][tg]["status"] = 0
            #             trn_match_metadata = 0

            #         vdr_trn = re.findall(r"\d+", trn_val)[0]
            #         if len(vdr_trn) > 12:
            #             trn_val = vdr_trn
            #         else:
            #             trn_val = ""
            #             dt["header"][tg][
            #                 "status_message"
            #             ] = "TRN is missing from Vendor metadata."
            #             dt["header"][tg]["status"] = 0
            #         # else:
            #         #     trn_match_metadata = 1
            #         if trn_val != "" and trn_val != ocr_trn:
            #             dt["header"][tg]["status_message"] = (
            #                 "TRN not matching with vendor metadata("
            #                 + str(trn_val)
            #                 + ")."
            #             )
            #             dt["header"][tg]["status"] = 0
            #             trn_match_metadata = 0

            #         else:

            #             dt["header"][tg]["status"] = 0
            #             dt["header"][tg][
            #                 "status_message"
            #             ] = "Low Confidence, Please Review."

            #     except Exception as trn_exp:
            #         logger.error(f"trn exception:{trn_exp} ")
            #         # print(trn_exp)
            #         dt["header"][tg]["status_message"] = (
            #             "Low Confidence, Meta data trn:" + str(trn_val) + ")."
            #         )
            #         dt["header"][tg]["status"] = 0

            # if dt["header"][tg]["tag"] == "PurchaseOrder":
            #     doc_po = dt["header"][tg]["data"]["value"]

            #     formatted_po, po_status = po_cvn(doc_po, entityID)
            #     dt["header"][tg]["data"]["value"] = formatted_po

            #     po_doc = (
            #         db.query(model.Document)
            #         .filter(
            #             model.Document.PODocumentID == formatted_po,
            #             model.Document.idDocumentType == 1,
            #         )
            #         .first()
            #     )
            #     if po_status == 1:
            #         dt["header"][tg]["status_message"] = "Please Review PO number."
            #         dt["header"][tg]["status"] = 0
            #     if po_doc is None:
            #         dt["header"][tg]["status_message"] = "PO not found in Serina!"
            #         dt["header"][tg]["status"] = 0

            try:
                # metaVendorName, metaVendorAdd match check
                if dt["header"][tg]["tag"] == "VendorName":
                    doc_VendorName = dt["header"][tg]["data"]["value"]

                if dt["header"][tg]["tag"] == "VendorAddress":
                    doc_VendorAddress = dt["header"][tg]["data"]["value"]

            except Exception as e:
                print(f"Except in vendor name and address: {str(e)}")

            if subtotal_Cal == 1:
                if dt["header"][tg]["tag"] == "InvoiceTotal":
                    invoiceTotal_rw = tb_cln_amt(dt["header"][tg]["data"]["value"])
                    if not isinstance(invoiceTotal_rw, float):
                        # dt['header'][tg]['data']['value']
                        dt["header"][tg][
                            "status_message"
                        ] = "Invalid Value, Please review"
                        dt["header"][tg]["status"] = 0
                        invoiceTotal_rw = ""

                if dt["header"][tg]["tag"] == "SubTotal":
                    subtotal_rw = tb_cln_amt(dt["header"][tg]["data"]["value"])
                    if not isinstance(subtotal_rw, float):
                        dt["header"][tg][
                            "status_message"
                        ] = "Invalid Value, Please review"
                        dt["header"][tg]["status"] = 0
                        subtotal_rw = ""

                if dt["header"][tg]["tag"] == "TotalDiscount":
                    totaldiscount_rw = tb_cln_amt(dt["header"][tg]["data"]["value"])
                    if not isinstance(totaldiscount_rw, float):
                        dt["header"][tg][
                            "status_message"
                        ] = "Invalid Value, Please review"
                        dt["header"][tg]["status"] = 0
                        totaldiscount_rw = ""

                if dt["header"][tg]["tag"] == "TotalTax":
                    totalTax_rw = tb_cln_amt(dt["header"][tg]["data"]["value"])
                    if not isinstance(totalTax_rw, float):
                        dt["header"][tg][
                            "status_message"
                        ] = "Invalid Value, Please review"
                        dt["header"][tg]["status"] = 0
                        totalTax_rw = ""
        try:

            vndMth_ck, vndMth_address_ck = VndMatchFn(
                metaVendorName, doc_VendorName, metaVendorAdd, doc_VendorAddress
            )
            logger.info(f"vndMth_ck: {vndMth_ck}")
        except Exception:
            logger.error(traceback.format_exc())
            vndMth_ck = 0
            vndMth_address_ck = 0

        for row_cnt in range(len(dt["tab"])):
            for rw in range(len(dt["tab"][row_cnt])):
                dt["tab"][row_cnt][rw]["row_count"] = row_cnt + 1
        SubTotal_data = ""
        # InvoiceTotal_val = ""  # TODO: Unused variable
        # TotalTax = ""  # TODO: Unused variable
        for tg in range(len(dt["header"])):

            if dt["header"][tg]["tag"] == "VendorName":
                doc_VendorName = dt["header"][tg]["data"]["value"]
                if vndMth_ck == 1:
                    dt["header"][tg][
                        "status_message"
                    ] = "Vendor Name Matching with Master Data"
                    dt["header"][tg]["status"] = 1

                else:
                    try:
                        ratio = fuzz.ratio(doc_VendorName, metaVendorName)
                        if ratio > 95:
                            dt["header"][tg][
                                "status_message"
                            ] = "Vendor Name Matching with Master Data"
                            dt["header"][tg]["status"] = 1
                        else:
                            vectorizer = TfidfVectorizer()

                            tfidf_matrix_di = vectorizer.fit_transform(
                                [doc_VendorName, metaVendorName]
                            )
                            cos_sim_vndName = cosine_similarity(
                                tfidf_matrix_di[0], tfidf_matrix_di[1]
                            )

                            if cos_sim_vndName[0][0] * 100 >= 95:
                                dt["header"][tg][
                                    "status_message"
                                ] = "Vendor Name Matching with Master Data"
                                dt["header"][tg]["status"] = 1

                                logger.info(
                                    f"cos_sim:{doc_VendorName} ,"
                                    + f"vendor:{metaVendorName}"
                                )

                            else:
                                dt["header"][tg][
                                    "status_message"
                                ] = "Vendor Name Mismatch with Master Data"
                                dt["header"][tg]["status"] = 0

                    except Exception as qw:
                        logger.error(f"vendor name exception: {str(qw)}")
                        logger.error(f" {traceback.format_exc()}")
                        dt["header"][tg][
                            "status_message"
                        ] = "Vendor Name Mismatch with Master Data"
                        dt["header"][tg]["status"] = 0

            if dt["header"][tg]["tag"] == "VendorAddress":

                if vndMth_address_ck == 1:
                    dt["header"][tg][
                        "status_message"
                    ] = "Vendor Address Matching with Master Data"
                    dt["header"][tg]["status"] = 1
                else:
                    dt["header"][tg][
                        "status_message"
                    ] = "Vendor Address Mismatch with Master Data"
                    dt["header"][tg]["status"] = 0

            if dt["header"][tg]["tag"] == "InvoiceTotal":
                dt["header"][tg]["data"]["value"] = cln_amt(
                    dt["header"][tg]["data"]["value"]
                )
                # InvoiceTotal_val = dt["header"][tg]["data"]["value"]
                # # TODO: Unused variable
                fr_data = dt
            if dt["header"][tg]["tag"] == "SubTotal":
                dt["header"][tg]["data"]["value"] = cln_amt(
                    dt["header"][tg]["data"]["value"]
                )
                SubTotal_data = dt["header"][tg]["data"]["value"]
                if subtotal_Cal == 1:
                    if (
                        (invoiceTotal_rw != "")
                        and (totalTax_rw != "")
                        and (totaldiscount_rw != "")
                        and (subtotal_rw != "")
                    ):
                        cal_subtotal_1 = invoiceTotal_rw - totalTax_rw
                        cal_subtotal_2 = subtotal_rw - totaldiscount_rw
                        if cal_subtotal_1 == cal_subtotal_2:
                            dt["header"][tg]["data"]["value"] = cal_subtotal_2
                        else:
                            dt["header"][tg]["status_message"] = (
                                "Calculation failed, Please update with (Invoice "
                                "Total - Total Tax) "
                            )
                            dt["header"][tg]["status"] = 0
                    else:
                        dt["header"][tg]["status_message"] = (
                            "Calculation failed, Please update with (Invoice "
                            "Total - Total Tax) "
                        )
                        dt["header"][tg]["status"] = 0

                fr_data = dt
            if dt["header"][tg]["tag"] == "TotalTax":
                dt["header"][tg]["data"]["value"] = cln_amt(
                    dt["header"][tg]["data"]["value"]
                )
                # TotalTax = dt["header"][tg]["data"]["value"]  # TODO: Unused variable
                fr_data = dt

        present_header = []
        missing_header = []

        for ck_hrd_tg in fr_data["header"]:
            present_header.append(ck_hrd_tg["tag"])

        try:
            if "InvoiceTotal" not in present_header:
                if SubTotal_data != "":
                    tmp = {}
                    # tmp['tag'] = 'InvoiceTotal'
                    tmp = {
                        "tag": "InvoiceTotal",
                        "data": {
                            "value": str(SubTotal_data),
                            "prebuilt_confidence": "0.0",
                            "custom_confidence": "0.0",
                        },
                        "bounding_regions": {"x": "", "y": "", "w": "", "h": ""},
                        "status": 1,
                        "status_message": "Calculated Value",
                    }
                    present_header.append("InvoiceTotal")
                    fr_data["header"].append(tmp)
        except Exception as e:
            logger.error(f"postpro line 1347: {str(e)}")
            logger.error(f" {traceback.format_exc()}")
            # (str(e))
        if not set(mandatory_header).issubset(set(present_header)):
            missing_header = list(set(mandatory_header) - set(present_header))
        for msg_itm_ck in missing_header:
            # notification missing header = msg_itm_ck
            tp_tg = {
                "tag": msg_itm_ck,
                "data": {
                    "value": "",
                    "prebuilt_confidence": "0.0",
                    "custom_confidence": "0.0",
                },
                "bounding_regions": {"x": "", "y": "", "w": "", "h": ""},
                "status": 0,
                "status_message": "Mandatory Headers Missing",
            }
            fr_data["header"].append(tp_tg)

        if len(missing_header) >= (len(mandatory_header)):
            # reject invoice
            fr_data = {}
            postprocess_msg = "Please check the document uploaded! - Model not found."
            postprocess_status = 0
            logger.error(
                "Please check the document uploaded! - Model not found:"
                + f"{postprocess_msg}"
            )

        else:
            labels_not_present = []
            present_tab_itm = []
            for tbl_tg_ck in fr_data["tab"]:
                for rw_tbl_tg_ck in tbl_tg_ck:
                    present_tab_itm.append(rw_tbl_tg_ck["tag"])

            tb_pt_cnt = dict(Counter(present_tab_itm))
            for mtc in mandatory_tab_col:
                if mtc in tb_pt_cnt:
                    if tb_pt_cnt[mtc] == len(fr_data["tab"]):
                        pass
                    else:
                        labels_not_present.append(mtc)

                else:
                    labels_not_present.append(mtc)
            # missing_tab_val = missing_rw_tab # Unused variable

    except Exception as e:
        fr_data = {}
        postprocess_status = 0
        postprocess_msg = str(e)
        logger.error(f" {traceback.format_exc()}")
        logger.error(f"postpro line 1833: {postprocess_msg}")

    try:
        sts_hdr_ck = 1
        if postprocess_status == 1:
            for cfd_ck in range(len(fr_data["header"])):

                if fr_data["header"][cfd_ck]["tag"] in mandatory_header:
                    try:
                        if (
                            float(fr_data["header"][0]["data"]["custom_confidence"])
                            * 100
                            < 0
                        ):
                            sts_hdr_ck = 0
                            logger.info(
                                "low confidence tag:"
                                + f"{fr_data['header'][cfd_ck]['tag']}"
                            )
                            _s = fr_data["header"][0]["data"]["custom_confidence"]
                            _sPercent = float(_s) * 100
                            logger.info("confidence score:" + f"{_sPercent}")
                    except Exception as tr:
                        logger.error(f"postpro line 1856: {str(tr)} ")
                        logger.error(f"postpro line 1856: {traceback.format_exc()}")
                        sts_hdr_ck = 0
        else:
            sts_hdr_ck = 0

    except Exception as qw:
        logger.error(f"postpro line 1431 exception: {qw} , {sts_hdr_ck}")
        logger.error(f" {traceback.format_exc()}")
        sts_hdr_ck = 0
    return fr_data, postprocess_msg, postprocess_status, duplicate_status, sts_hdr_ck
