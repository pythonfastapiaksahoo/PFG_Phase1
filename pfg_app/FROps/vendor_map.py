import json
from Levenshtein import ratio as levenshtein_ratio
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import json
from pfg_app.logger_module import logger
import pfg_app.model as model
from pfg_app.session.session import SQLALCHEMY_DATABASE_URL, get_db
import time
import traceback
from datetime import datetime, timezone
from pfg_app.core.stampData import VndMatchFn_corp
from pfg_app.crud.CorpIntegrationCrud import corp_update_docHistory

# def compute_cosine_similarity(text1, text2):
#     if not text1 or not text2:
#         return 0  # Return 0 if either text is empty or None
#     vectorizer = TfidfVectorizer().fit_transform([text1, text2])
#     vectors = vectorizer.toarray()
#     return cosine_similarity([vectors[0]], [vectors[1]])[0][0] * 100  # Convert to percentage

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def compute_cosine_similarity(text1, text2):
    if not text1 or not text2 or text1.strip() == "" or text2.strip() == "":
        return 0  # Return 0 if either text is empty or None
    
    try:
        vectorizer = TfidfVectorizer()
        vectors = vectorizer.fit_transform([text1, text2])
        
        # If there's only one unique word (or no meaningful words), prevent error
        if vectors.shape[1] == 0:
            return 0

        vectors = vectors.toarray()
        return cosine_similarity([vectors[0]], [vectors[1]])[0][0] * 100  # Convert to percentage

    except ValueError as e:
        print(f"Error in TF-IDF vectorization: {e} | Text1: '{text1}', Text2: '{text2}'")
        return 0  # Handle exception gracefully


# Thresholds
LEVENSHTEIN_NAME_THRESHOLD = 90
LEVENSHTEIN_ADDR_THRESHOLD = 60
COSINE_NAME_THRESHOLD = 95
COSINE_ADDR_THRESHOLD = 60

import json

def find_best_vendor_match_onboarded(openai_vendor_name, openai_vendor_address, corp_metadata_df):
    matching_vendors = {}

    # Ensure inputs are strings and lowercase
    openai_vendor_name = str(openai_vendor_name or "").lower()
    openai_vendor_address = str(openai_vendor_address or "").lower()

    for syn_nm, v_id, vendorName, vendorAddress, syn_add, vrd_cd in zip(
        corp_metadata_df["synonyms_name"],
        corp_metadata_df["vendorid"],
        corp_metadata_df["vendorname"],
        corp_metadata_df["vendoraddress"],
        corp_metadata_df["synonyms_address"],
        corp_metadata_df["vendorcode"],
    ):

        vendorName = str(vendorName or "").lower()
        vendorAddress = str(vendorAddress or "").lower()

        # Compute similarity scores for vendor name
        levenshtein_score_name_1 = levenshtein_ratio(openai_vendor_name, vendorName) * 100
        cosine_score_name_2 = compute_cosine_similarity(openai_vendor_name, vendorName)

        levenshtein_score_name_3 = 0
        cosine_score_name_4 = 0
        if syn_nm and str(syn_nm) != "None":
            try:
                synonyms_nm = json.loads(syn_nm)
                if isinstance(synonyms_nm, list):
                    for syn_2 in synonyms_nm:
                        syn2 = str(syn_2).strip().lower()
                        levenshtein_score_name_3 = max(levenshtein_score_name_3, levenshtein_ratio(openai_vendor_name, syn2) * 100)
                        cosine_score_name_4 = max(cosine_score_name_4, compute_cosine_similarity(openai_vendor_name, syn2))
            except json.JSONDecodeError:
                pass  # Ignore invalid JSON

        # Compute similarity scores for vendor address
        levenshtein_score_addr_1 = levenshtein_ratio(openai_vendor_address, vendorAddress) * 100
        cosine_score_addr_2 = compute_cosine_similarity(openai_vendor_address, vendorAddress)

        levenshtein_score_addr_3 = 0
        cosine_score_addr_4 = 0
        if syn_add and str(syn_add) != "None":
            try:
                synonyms_add = json.loads(syn_add)
                if isinstance(synonyms_add, list):
                    for syn_2 in synonyms_add:
                        syn2 = str(syn_2).strip().lower()
                        levenshtein_score_addr_3 = max(levenshtein_score_addr_3, levenshtein_ratio(openai_vendor_address, syn2) * 100)
                        cosine_score_addr_4 = max(cosine_score_addr_4, compute_cosine_similarity(openai_vendor_address, syn2))
            except json.JSONDecodeError:
                pass  # Ignore invalid JSON

        # Use the highest match found
        max_levenshtein_name = max(levenshtein_score_name_1, levenshtein_score_name_3)
        max_cosine_name = max(cosine_score_name_2, cosine_score_name_4)
        max_levenshtein_addr = max(levenshtein_score_addr_1, levenshtein_score_addr_3)
        max_cosine_addr = max(cosine_score_addr_2, cosine_score_addr_4)

        levenshtein_name_match = max_levenshtein_name >= LEVENSHTEIN_NAME_THRESHOLD
        levenshtein_addr_match = max_levenshtein_addr >= LEVENSHTEIN_ADDR_THRESHOLD
        cosine_name_match = max_cosine_name >= COSINE_NAME_THRESHOLD
        cosine_addr_match = max_cosine_addr >= COSINE_ADDR_THRESHOLD

        match_score = {
            "vendor_id": v_id,
            "vendor_name": vendorName,
            "vendor_address": vendorAddress,
            "vendor_code": vrd_cd,
            "scores": {
                "levenshtein_score_name": round(max_levenshtein_name, 2),
                "cosine_score_name": round(max_cosine_name, 2),
                "levenshtein_score_addr": round(max_levenshtein_addr, 2),
                "cosine_score_addr": round(max_cosine_addr, 2),
            },
        }

        if (levenshtein_name_match or cosine_name_match) and (levenshtein_addr_match or cosine_addr_match):
            match_score["bestmatch"] = "Full Match"
        elif levenshtein_name_match or cosine_name_match:
            match_score["bestmatch"] = "Name Match"
        elif levenshtein_addr_match or cosine_addr_match:
            match_score["bestmatch"] = "Address Match"
        else:
            continue  # Skip if no match at all

        matching_vendors[v_id] = match_score

    return matching_vendors


def find_best_vendor_match_not_onboarded(openai_vendor_name, openai_vendor_address, vendorName_df):
    matching_vendors = {}

    openai_vendor_name = openai_vendor_name or ""
    openai_vendor_address = openai_vendor_address or ""

    for v_id, vendorName, vendorAddress, vrd_cd in zip(
        vendorName_df["idVendor"],
        vendorName_df["VendorName"],
        vendorName_df["Address"],
        vendorName_df["VendorCode"]
    ):

        vendorName = vendorName or ""  
        vendorAddress = vendorAddress or ""

        # Compute similarity scores for vendor name
        levenshtein_score_name_1 = levenshtein_ratio(openai_vendor_name.lower(), vendorName.lower()) * 100
        cosine_score_name_2 = compute_cosine_similarity(openai_vendor_name, vendorName)

        # Compute similarity scores for vendor address
        levenshtein_score_addr_1 = levenshtein_ratio(openai_vendor_address.lower(), vendorAddress.lower()) * 100
        cosine_score_addr_2 = compute_cosine_similarity(openai_vendor_address, vendorAddress)

        # Check if name and address match above thresholds
        levenshtein_name_match = levenshtein_score_name_1 >= LEVENSHTEIN_NAME_THRESHOLD
        levenshtein_addr_match = levenshtein_score_addr_1 >= LEVENSHTEIN_ADDR_THRESHOLD
        cosine_name_match = cosine_score_name_2 >= COSINE_NAME_THRESHOLD
        cosine_addr_match = cosine_score_addr_2 >= COSINE_ADDR_THRESHOLD

        match_score = {
            "vendor_id": v_id,
            "vendor_name": vendorName,
            "vendor_address": vendorAddress,
            "vendor_code": vrd_cd,
            "scores": {
                "levenshtein_score_name_1": round(levenshtein_score_name_1, 2),
                "cosine_score_name_2": round(cosine_score_name_2, 2),
                "levenshtein_score_addr_1": round(levenshtein_score_addr_1, 2),
                "cosine_score_addr_2": round(cosine_score_addr_2, 2),
            },
        }

        if levenshtein_name_match and levenshtein_addr_match and cosine_name_match and cosine_addr_match:
            match_score["bestmatch"] = "Full Match"
        elif levenshtein_name_match or cosine_name_match:
            match_score["bestmatch"] = "Name Match"
        elif levenshtein_addr_match or cosine_addr_match:
            match_score["bestmatch"] = "Address Match"
        else:
            continue  # Skip if no match at all

        matching_vendors[v_id] = match_score

    return matching_vendors


def matchVendorCorp(openai_vendor_name,openai_vendor_address,corp_metadata_df,vendorName_df, userID,docID,db):
    # userID = 1
    logger.info(f"Matching vendor corp: openai_vendor_name: {openai_vendor_name}, openai_vendor_address: {openai_vendor_address}, docID: {docID}")
    vndMth_address_ck = ""
    notOnboarded = 0
    vendorFound = 0
    openAIcall_required = 0
    vendorNotFound = 0
    NotOnboarded_matching_vendors = {}
    matching_vendors = {}
    matching_vendors = find_best_vendor_match_onboarded(openai_vendor_name, openai_vendor_address, corp_metadata_df)
    logger.info(f"Matching vendor corp: matching_vendors length: {len(matching_vendors)}")

    if len(matching_vendors)==1:
        if matching_vendors[(list(matching_vendors.keys())[0])]["bestmatch"]=='Full Match':
            vendorFound=1
        elif matching_vendors[(list(matching_vendors.keys())[0])]["bestmatch"]=='Name Match':
            vendorFound=1
        # else:
        #     openAIcall_required = 1
    elif len(matching_vendors)==0:
        # vendor not onboarded
        vendorFound = 0
        openAIcall_required = 0
    elif len(matching_vendors)>1:
        openAIcall_required = 1
        vendorFound = 0
    logger.info(f"line 207-vendorFound:{vendorFound},openAIcall_required: {openAIcall_required}, ")
    if vendorFound==1:
        vendorNotFound = 0
        notOnboarded = 0
        
        # map vendor
        matched_id_vendor = matching_vendors[list(matching_vendors.keys())[0]]["vendor_id"]
        vendorID = matched_id_vendor
        vrd_cd = matching_vendors[list(matching_vendors.keys())[0]]["vendor_code"]

        docStatus = 4
        documentdesc = f"Vendor match found:{vendorID}"
        substatus = 11
        corp_update_docHistory(docID, userID, docStatus, documentdesc, db,substatus)
        
        db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == docID
        ).update(
            {
                model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                model.corp_document_tab.documentsubstatus: substatus,  # noqa: E501
                model.corp_document_tab.last_updated_by: userID,
                model.corp_document_tab.vendor_id: vendorID,
                model.corp_document_tab.vendor_code:vrd_cd

            }
        )
        db.commit()

    elif (openAIcall_required == 0) and (vendorFound==0):
        NotOnboarded_matching_vendors = find_best_vendor_match_not_onboarded(openai_vendor_name, openai_vendor_address, vendorName_df)
        logger.info(f"NotOnboarded_matching_vendors: {NotOnboarded_matching_vendors}")
        if len(NotOnboarded_matching_vendors) == 1:
            if NotOnboarded_matching_vendors[(list(NotOnboarded_matching_vendors.keys())[0])]["bestmatch"] == 'Full Match' :
                if NotOnboarded_matching_vendors[(list(NotOnboarded_matching_vendors.keys())[0])]["bestmatch"] == 'Name Match' :
                    print("Name Match only") 
                    vendorFound  = 1
                    notOnboarded = 1
            else:
                openAIcall_required = 0
        elif len(NotOnboarded_matching_vendors)==0:
                # vendor not found
            vendorFound = 0
            vendorNotFound = 1
            openAIcall_required = 0
        elif len(NotOnboarded_matching_vendors)>1:
            openAIcall_required = 1
            vendorFound = 0

    if vendorNotFound==1:
        # vendor not found: docStatus = 26 & substatus = 107
        docStatus = 26
        substatus = 107
        documentdesc = "vendor not found"
        corp_update_docHistory(docID, userID, docStatus, documentdesc, db,docStatus)
        
        
        db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == docID
            ).update(
                {
                    model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                    model.corp_document_tab.documentsubstatus: substatus,  # noqa: E501
                    model.corp_document_tab.last_updated_by: userID,
                    model.corp_document_tab.vendor_id:0,
                }
            )
        db.commit()
    elif notOnboarded==1:
        # vendor not onboarded: docStatus = 25 & substatus = 106
        docStatus = 25
        substatus = 106
        documentdesc = "vendor not onboarded"
        corp_update_docHistory(docID, userID, docStatus, documentdesc, db,substatus)
        
        db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == docID
            ).update(
                {
                    model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                    model.corp_document_tab.documentsubstatus: substatus,  # noqa: E501
                    model.corp_document_tab.last_updated_by: userID,
                    model.corp_document_tab.vendor_id:0,
                }
            )
        db.commit()
        
    elif openAIcall_required==1:
        if len(matching_vendors)>1:
            # openAI call with matching_vendors
            vndMth_address_ck, matched_id_vendor = VndMatchFn_corp(openai_vendor_name, openai_vendor_address, matching_vendors)
            # matching_vendors
            
        elif len(NotOnboarded_matching_vendors)>1:
            # openAI call with NotOnboarded_matching_vendors
            vndMth_address_ck, matched_id_vendor = VndMatchFn_corp(openai_vendor_name, openai_vendor_address, matching_vendors)
        if vndMth_address_ck=='yes':
            vendorID = matched_id_vendor
            docStatus = 4
            substatus = 11
            db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == docID
            ).update(
                {
                    model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                    model.corp_document_tab.documentsubstatus: substatus,  # noqa: E501
                    model.corp_document_tab.last_updated_by: userID,
                    model.corp_document_tab.vendor_id: vendorID,

                }
            )
            db.commit()

            # "vendormatchfound": "yes" or "no",  
            #                           "vendorID": "matching_vendor_id" or ""  
            
    else:
        if vendorFound!=1:
            # update as vendorNotFound:docStatus = 26 & substatus = 107
            docStatus = 26
            substatus = 107
            documentdesc = "vendor not found"

            corp_update_docHistory(docID, userID, docStatus, documentdesc, db,substatus)
            
            db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == docID
                ).update(
                    {
                        model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                        model.corp_document_tab.documentsubstatus: substatus,  # noqa: E501
                        model.corp_document_tab.last_updated_by: userID,
                        model.corp_document_tab.vendor_id:0,
                    }
                )
            db.commit()
            logger.info(f"line 334-vendorFound:{vendorFound} ")
    return 
            
            
        
        
