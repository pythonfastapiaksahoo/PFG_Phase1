"""
1. Fetch unprocessed data
2. Convert into required table structure with required columns
3.Po VS Invo:
    i. For every item in invo, get score matrix of fuzzy match
    ii. Map the hightes score item if its above the defined treshold
    iii. Divide the data into matched_items, missing_items, unknown_items
        - matched_items - items matched in both with qty
        - missing_items - items in PO but not in Invo,including qty mismatch
        - unknown_items - items in invo not in PO - when invo item code is not mapped with any po item code
5. GRN vs PO:
    i. Get matched_items and chech with GRN QUANTITY_RECEIVED, and output the GRN status
    ii. missing_items - Cross verify with QUANTITY_RECEIVED, and adjust the values,with 0 status
    iii. unknown_items

"""

import sys
import time

import pandas as pd
from fuzzywuzzy import fuzz
from session.session import DB, HOST, PORT, PWD, USR

sys.path.append("..")


def invo_process(
    po_doc_id,
    invo_doc_id,
    documentLineitem_po_df,
    doc_header_po_df,
    documentLineitem_invo_df,
    doc_header_invo_df,
    documentLineitem_grn_df,
    doc_inline_tags,
    ck_threshold,
    qty_threshold,
):
    doc_status = 0
    po_match_status = 1
    # po_doc_id = 2737868
    po_inlinedf = documentLineitem_po_df[
        documentLineitem_po_df["documentID"] == po_doc_id
    ]
    po_header_df = doc_header_po_df[doc_header_po_df["documentID"] == po_doc_id]
    # for loop w.r.t docID in place of 2737802

    # invo_doc_id = 2737956
    invo_inlinedf = documentLineitem_invo_df[
        documentLineitem_invo_df["documentID"] == invo_doc_id
    ]
    invo_header_df = doc_header_invo_df[doc_header_invo_df["documentID"] == invo_doc_id]
    req_invo_inline = invo_inlinedf[
        ["idDocumentLineItems", "lineItemtagID", "Value", "itemCode"]
    ].merge(doc_inline_tags, on="lineItemtagID", how="left")
    req_po_inline = po_inlinedf[
        ["idDocumentLineItems", "lineItemtagID", "Value", "itemCode"]
    ].merge(doc_inline_tags, on="lineItemtagID", how="left")
    # req_invo_inline['itemCode']=[1,1,1,1,1,2,2,2,2,2,3,3,3,3,3,4,4,4,4,4,5,5,5,5,5,6,6,6,6,6,7,7,7,7,7]

    # PO DATA
    po_grouped_df = req_po_inline.groupby("TagName")
    po_desc = po_grouped_df.get_group("ITEM_DESCRIPTION")["Value"]
    po_item_code = po_grouped_df.get_group("ITEM_DESCRIPTION")["itemCode"]
    po_desc_df = pd.DataFrame(
        list(zip(po_desc, po_item_code)), columns=["ITEM_DESCRIPTION", "itemCode"]
    )
    po_desc_df["itemCode"] = po_desc_df["itemCode"].astype("str")
    po_qty = po_grouped_df.get_group("QUANTITY")["Value"]
    po_qty_item_code = po_grouped_df.get_group("QUANTITY")["itemCode"]
    po_qty_df = pd.DataFrame(
        list(zip(po_qty, po_qty_item_code)), columns=["QUANTITY", "itemCode"]
    )
    po_qty_df["itemCode"] = po_qty_df["itemCode"].astype("str")
    po_Uty_price = po_grouped_df.get_group("UNIT_PRICE")["Value"]
    po_Uty_price_item_code = po_grouped_df.get_group("UNIT_PRICE")["itemCode"]
    po_Uty_price_df = pd.DataFrame(
        list(zip(po_Uty_price, po_Uty_price_item_code)),
        columns=["UNIT_PRICE", "itemCode"],
    )
    po_Uty_price_df["itemCode"] = po_Uty_price_df["itemCode"].astype("str")
    po_tab = po_desc_df.merge(po_qty_df, on="itemCode").merge(
        po_Uty_price_df, on="itemCode"
    )
    po_tab["QUANTITY"] = po_tab["QUANTITY"].astype("float")
    po_tab["UNIT_PRICE"] = po_tab["UNIT_PRICE"].astype("float")

    # INVO DATA
    inv_grouped_df = req_invo_inline.groupby("TagName")
    desc = inv_grouped_df.get_group("Description")["Value"]
    item_code = inv_grouped_df.get_group("Description")["itemCode"]
    desc_df = pd.DataFrame(
        list(zip(desc, item_code)), columns=["Description", "itemCode"]
    )
    # unt = inv_grouped_df.get_group('Unit')['Value']
    # unt_item_code =  inv_grouped_df.get_group('Unit')['itemCode']
    # unt_df = pd.DataFrame(list(zip(unt,unt_item_code)),columns=['Unit','itemCode'])
    qty = inv_grouped_df.get_group("Quantity")["Value"]
    qty_item_code = inv_grouped_df.get_group("Quantity")["itemCode"]
    qty_df = pd.DataFrame(
        list(zip(qty, qty_item_code)), columns=["Quantity", "itemCode"]
    )
    Uty_price = inv_grouped_df.get_group("UnitPrice")["Value"]
    Uty_price_item_code = inv_grouped_df.get_group("UnitPrice")["itemCode"]
    Uty_price_df = pd.DataFrame(
        list(zip(Uty_price, Uty_price_item_code)), columns=["UnitPrice", "itemCode"]
    )
    Amt = inv_grouped_df.get_group("Amount")["Value"]
    Amt_item_code = inv_grouped_df.get_group("Amount")["itemCode"]
    Amt_df = pd.DataFrame(list(zip(Amt, Amt_item_code)), columns=["Amount", "itemCode"])
    invo_tab = (
        desc_df.merge(qty_df, on="itemCode")
        .merge(Uty_price_df, on="itemCode")
        .merge(Amt_df, on="itemCode")
    )
    invo_tab["Quantity"] = invo_tab["Quantity"].astype("float")
    invo_tab["UnitPrice"] = invo_tab["UnitPrice"].astype("float")

    # GRN DATA

    PO_HEADER_ID = list(
        po_header_df[po_header_df["TagLabel"] == "PO_HEADER_ID"]["Value"]
    )[0]

    # [documentLineitem_grn_df['documentID'] == 2737793]
    grn_inlinedf = documentLineitem_grn_df
    req_grn_df = grn_inlinedf[
        ["idDocumentLineItems", "documentID", "lineItemtagID", "Value", "itemCode"]
    ].merge(doc_inline_tags, on="lineItemtagID", how="left")
    temp_grndf = req_grn_df[req_grn_df["TagName"] == "PO_HEADER_ID"]
    req_grn_docID = temp_grndf[temp_grndf["Value"] == PO_HEADER_ID][
        "documentID"
    ].unique()[0]
    grn_df = req_grn_df[req_grn_df["documentID"] == req_grn_docID]

    grn_grouped_df = grn_df.groupby("TagName")
    grn_po_unit_price = grn_grouped_df.get_group("PO_UNIT_PRICE")["Value"]
    grn_po_item_code = grn_grouped_df.get_group("PO_UNIT_PRICE")["itemCode"]
    grn_po_unitprice_df = pd.DataFrame(
        list(zip(grn_po_unit_price, grn_po_item_code)),
        columns=["PO_UNIT_PRICE", "itemCode"],
    )
    grn_qty = grn_grouped_df.get_group("QUANTITY")["Value"]
    grn_qty_item_code = grn_grouped_df.get_group("QUANTITY")["itemCode"]
    grn_qty_df = pd.DataFrame(
        list(zip(grn_qty, grn_qty_item_code)), columns=["QUANTITY", "itemCode"]
    )
    grn_qty_accepted = grn_grouped_df.get_group("QUANTITY_ACCEPTED")["Value"]
    grn_qty_accepted_item_code = grn_grouped_df.get_group("QUANTITY_ACCEPTED")[
        "itemCode"
    ]
    grn_qty_accepted_df = pd.DataFrame(
        list(zip(grn_qty_accepted, grn_qty_accepted_item_code)),
        columns=["QUANTITY_ACCEPTED", "itemCode"],
    )
    grn_qty_cancelled = grn_grouped_df.get_group("QUANTITY_CANCELLED")["Value"]
    grn_qty_cancelled_item_code = grn_grouped_df.get_group("QUANTITY_CANCELLED")[
        "itemCode"
    ]
    grn_qty_cancelled_df = pd.DataFrame(
        list(zip(grn_qty_cancelled, grn_qty_cancelled_item_code)),
        columns=["QUANTITY_CANCELLED", "itemCode"],
    )
    grn_qty_received = grn_grouped_df.get_group("QUANTITY_RECEIVED")["Value"]
    grn_qty_received_item_code = grn_grouped_df.get_group("QUANTITY_RECEIVED")[
        "itemCode"
    ]
    grn_qty_received_df = pd.DataFrame(
        list(zip(grn_qty_received, grn_qty_received_item_code)),
        columns=["QUANTITY_RECEIVED", "itemCode"],
    )

    grn_po_line_id = grn_grouped_df.get_group("PO_LINE_ID")["Value"]
    grn_po_line_id_item_code = grn_grouped_df.get_group("PO_LINE_ID")["itemCode"]
    grn_po_line_id_df = pd.DataFrame(
        list(zip(grn_po_line_id, grn_po_line_id_item_code)),
        columns=["PO_LINE_ID", "itemCode"],
    )

    grn_tab = (
        grn_po_unitprice_df.merge(grn_qty_df, on="itemCode")
        .merge(grn_qty_accepted_df, on="itemCode")
        .merge(grn_qty_cancelled_df, on="itemCode")
        .merge(grn_qty_received_df, on="itemCode")
        .merge(grn_po_line_id_df, on="itemCode")
    )

    ck_matrix = {}
    for cmp in range(len(invo_tab)):
        ck_decp = invo_tab["Description"][cmp].split("-")[0]
        tmp_mxt = {}
        for cmp_po in range(len(po_tab)):
            ck_po_decp = po_tab["ITEM_DESCRIPTION"][cmp_po]
            fuz_ratio = fuzz.ratio(ck_decp.lower(), ck_po_decp.lower())
            # print(fuz_ratio)
            tmp_mxt[po_tab["itemCode"][cmp_po]] = fuz_ratio
        ck_matrix[invo_tab["itemCode"][cmp]] = tmp_mxt

    item_code_map = {}
    temp_code_map = {}
    matched_items = {}  # items which matched with PO
    missing_items = {}  # items in PO,but not in invo(including qty mismatch)
    unknown_items = {}  # items in invo,but not in PO

    for invo_itm_cd in ck_matrix:  # invo_itm_cd == invo itemCode
        # ck_matrix[invo_itm_cd]
        temp_code_map = {}
        mx_scr_item = max(
            ck_matrix[invo_itm_cd], key=ck_matrix[invo_itm_cd].get
        )  # mx_scr_item == po item code
        mx_scr = ck_matrix[invo_itm_cd][mx_scr_item]

        po_qty = float(po_tab[po_tab["itemCode"] == mx_scr_item]["QUANTITY"])
        invo_qty = float(invo_tab[invo_tab["itemCode"] == invo_itm_cd]["Quantity"])
        if mx_scr >= ck_threshold:
            print("1. matching: item descp - ")
            status_scr = 0
            po_match_status_msg = "PO Quantity mismatched with Invo Quantity"
            if (po_qty == invo_qty) or (abs(po_qty - invo_qty) <= qty_threshold):
                print("2. matching: item qty -  ")
                status_scr = 1
                po_match_status = po_match_status * 1
                po_match_status_msg = "PO Quantity matched with Invo Quantity"
            else:
                po_match_status = po_match_status * 0
                print("2. matching: item qty - X ")
                status_scr = 0
                po_match_status_msg = "PO Quantity mismatched with Invo Quantity"
                temp_code_map[mx_scr_item] = {
                    "invo_itm_code": invo_itm_cd,
                    "fuzz_scr": mx_scr,
                    "status": status_scr,
                    "po_qty": po_qty,
                    "invo_qty": invo_qty,
                    "qty_mismatch": (po_qty - invo_qty),
                    "po_match_status_msg": po_match_status_msg,
                }
                # missing_items[mx_scr_item] = temp_code_map
                missing_items.update(temp_code_map)

            temp_code_map[mx_scr_item] = {
                "invo_itm_code": invo_itm_cd,
                "fuzz_scr": mx_scr,
                "invo_qty": invo_qty,
                "po_qty": po_qty,
                "status": status_scr,
                "qty_mismatch": (po_qty - invo_qty),
                "po_match_status_msg": po_match_status_msg,
            }
            # matched_items[mx_scr_item] = temp_code_map
            matched_items.update(temp_code_map)

        else:
            # print('1. matching: item descp - X')
            status_scr = 0
            po_match_status = po_match_status * 0
            po_match_status_msg = (
                "Invo item not matching with PO item with defined threshold"
            )
            temp_code_map[mx_scr_item] = {
                "invo_itm_code": invo_itm_cd,
                "fuzz_scr": mx_scr,
                "status": status_scr,
                "po_qty": po_qty,
                "invo_qty": invo_qty,
                "qty_mismatch": (invo_qty),
                "po_match_status_msg": po_match_status_msg,
            }
            # unknown_items[mx_scr_item] = temp_code_map
            unknown_items.update(temp_code_map)
            print(ck_matrix[invo_itm_cd])

        item_code_map[mx_scr_item] = {
            "invo_itm_code": invo_itm_cd,
            "fuzz_scr": mx_scr,
            "status": status_scr,
            "invo_qty": invo_qty,
        }

    missing_items_list = list(
        set(list(item_code_map.keys())) - set(list(po_tab["itemCode"]))
    )
    if len(missing_items) > 0:
        po_match_status = po_match_status * 0
        print("item_missing: ", missing_items_list)
    else:
        print("happy, All good!")

    if po_match_status == 1:
        item_code_map_status = 1
        item_code_map_status_msg = "All Invo items mapped with PO items"
    else:
        item_code_map_status = 0
        item_code_map_status_msg = "Item map missing"

    item_code_map["item_code_map_status"] = item_code_map_status
    item_code_map["item_code_map_status_msg"] = item_code_map_status_msg

    grn_qty_status = {}
    over_all_grn_status = 1

    grn_status = 0
    for grn_itm in range(len(grn_qty_received_df["itemCode"])):
        grn_itm_code = grn_qty_received_df["itemCode"][grn_itm]
        grn_qty = grn_qty_received_df["QUANTITY_RECEIVED"][grn_itm]
        print(grn_qty)
        invo_qty_ck = ""
        if str(grn_itm_code) in item_code_map.keys():
            invo_qty_ck = item_code_map[str(grn_itm_code)]["invo_qty"]
            if float(grn_qty) == float(invo_qty_ck):
                grn_status = 1
                grn_status_msg = "GRN Quantity matched with Invo Quantity"
            else:
                over_all_grn_status = over_all_grn_status * 0
                grn_status = 0
                grn_status_msg = "GRN Quantity mismatched with Invo Quantity"
        else:
            over_all_grn_status = over_all_grn_status * 0
            grn_status = 0
            grn_status_msg = "Invo item not found in GRN item list"

        grn_qty_status[grn_itm_code] = {
            "grn_status": grn_status,
            "grn_status_msg": grn_status_msg,
            "PO_qty": po_qty,
            "invoice_qty": invo_qty_ck,
            "grn_qty": grn_qty,
        }

    if over_all_grn_status == 1:
        grn_ovl_sts = 1
        grn_ovl_sts_msg = "GRN check Success"
    else:
        grn_ovl_sts = 0
        grn_ovl_sts_msg = "GRN check Failed"

    grn_qty_status["grn_overall status:"] = grn_ovl_sts
    grn_qty_status["grn_overall status msg:"] = grn_ovl_sts_msg

    if (grn_status == 1) and (po_match_status == 1):
        doc_status = 2
        doc_status_msg = "Approval In-Process"
    else:
        doc_status = 4
        doc_status_msg = "Exception discovered, manual check required"

    final_invo_pro_data = {
        "item_code_map": item_code_map,
        "grn_qty_status": grn_qty_status,
        "missing_items": missing_items,
        "unknown_items": unknown_items,
        "doc_status": doc_status,
        "doc_status_msg": doc_status_msg,
    }

    return final_invo_pro_data


SQL_USER = USR
SQL_PASS = PWD

localhost = HOST
SQL_DB = DB
SQL_PORT = PORT
ck_threshold = 40
qty_threshold = 0.9


def invo_bch_pro(
    SQL_USER, SQL_PASS, localhost, SQL_DB, SQL_PORT, ck_threshold, qty_threshold
):
    SQLALCHEMY_DATABASE_URL = (
        f"mysql+pymysql://{SQL_USER}:{SQL_PASS}@{localhost}:{SQL_PORT}/{SQL_DB}"
    )
    time.sleep(2)
    documentdata_df = pd.read_sql_table("documentData", SQLALCHEMY_DATABASE_URL)
    time.sleep(1)
    documentTagdef_df = pd.read_sql_table("documentTagdef", SQLALCHEMY_DATABASE_URL)
    time.sleep(1)
    doc_tags_ids = documentTagdef_df[["idDocumentTagDef", "TagLabel"]]
    doc_data = documentdata_df[["documentTagDefID", "Value", "documentID"]]
    doc_data.columns = ["idDocumentTagDef", "Value", "documentID"]
    doc_header_data = doc_data.merge(doc_tags_ids, on="idDocumentTagDef", how="left")
    documentType_df = pd.read_sql_table("documentType", SQLALCHEMY_DATABASE_URL)
    time.sleep(1)
    document_df = pd.read_sql_table("document", SQLALCHEMY_DATABASE_URL)

    docType_df = documentType_df[["idDocumentType", "Name"]]
    doc_df = document_df[
        ["idDocumentType", "idDocument", "documentStatusID", "PODocumentID"]
    ]

    doc_ipType_df = docType_df.merge(doc_df, on="idDocumentType", how="left")

    doc_ipType_df.columns = [
        "idDocumentType",
        "Name",
        "documentID",
        "documentStatusID",
        "PODocumentID",
    ]

    doc_header_data_df = doc_header_data.merge(
        doc_ipType_df, on="documentID", how="left"
    )

    documentLineitems_df = pd.read_sql_table(
        "documentLineitems", SQLALCHEMY_DATABASE_URL
    )
    time.sleep(1)
    documentLineitemtags_df = pd.read_sql_table(
        "documentLineitemtags", SQLALCHEMY_DATABASE_URL
    )
    time.sleep(1)
    doc_inline_tags = documentLineitemtags_df[["idDocumentLineItemTags", "TagName"]]
    doc_inline_data = documentLineitems_df[["lineItemtagID", "Value", "documentID"]]
    doc_inline_tags.columns = ["lineItemtagID", "TagName"]
    doc_inline_data = doc_inline_data.merge(
        doc_inline_tags, on="lineItemtagID", how="left"
    )
    documentLineitem_df = documentLineitems_df.merge(
        doc_ipType_df, on="documentID", how="left"
    )

    doc_header_po_df = doc_header_data_df[doc_header_data_df["idDocumentType"] == 1]
    doc_header_data_df = doc_header_data_df.reset_index(drop=True)
    documentLineitem_po_df = documentLineitem_df[
        documentLineitem_df["idDocumentType"] == 1
    ]
    documentLineitem_po_df = documentLineitem_po_df.reset_index(drop=True)
    doc_header_invo_df = doc_header_data_df[doc_header_data_df["idDocumentType"] == 3]
    doc_header_invo_df = doc_header_invo_df.reset_index(drop=True)
    documentLineitem_invo_df = documentLineitem_df[
        documentLineitem_df["idDocumentType"] == 3
    ]
    documentLineitem_df = documentLineitem_df.reset_index(drop=True)
    documentLineitem_grn_df = documentLineitem_df[
        documentLineitem_df["idDocumentType"] == 2
    ]
    documentLineitem_grn_df = documentLineitem_grn_df.reset_index(drop=True)

    status_doc_invodf = doc_header_invo_df[doc_header_invo_df["documentStatusID"] == 1]
    status_doc_invodf = status_doc_invodf.reset_index(drop=True)
    status_doc_invodf["PODocumentID"] = status_doc_invodf["PODocumentID"].fillna(0)
    req_doc_header_invo_df = status_doc_invodf[
        status_doc_invodf["PODocumentID"] != 0
    ].reset_index(drop=True)
    tmp_req_doc_header_invo_df = req_doc_header_invo_df[
        req_doc_header_invo_df["PODocumentID"] > 2737861
    ]
    tmp_req_doc_header_invo_df = tmp_req_doc_header_invo_df[
        tmp_req_doc_header_invo_df["TagLabel"] == "InvoiceId"
    ].reset_index(drop=True)
    ponum_df = doc_header_po_df[doc_header_po_df["TagLabel"] == "SEGMENT1"]
    invo_pro_data = {}
    for inv_id in range(len(tmp_req_doc_header_invo_df)):
        invo_doc_id = int(tmp_req_doc_header_invo_df["documentID"][inv_id])
        # po_doc_id = int(tmp_req_doc_header_invo_df['PODocumentID'][inv_id])
        selected_po = tmp_req_doc_header_invo_df["PODocumentID"][inv_id]
        po_doc_info = ponum_df[ponum_df["Value"] == selected_po]
        po_doc_id = int(po_doc_info["documentID"])

        print(po_doc_id, invo_doc_id)
        final_invo_pro_data = invo_process(
            po_doc_id,
            invo_doc_id,
            documentLineitem_po_df,
            doc_header_po_df,
            documentLineitem_invo_df,
            doc_header_invo_df,
            documentLineitem_grn_df,
            doc_inline_tags,
            ck_threshold,
            qty_threshold,
        )
        invo_pro_data[invo_doc_id] = final_invo_pro_data
        print("final_invo_pro_data: ", final_invo_pro_data)

    return invo_pro_data
