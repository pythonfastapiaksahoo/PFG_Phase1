import json
from datetime import datetime
from pfg_app import model

def to_dict(obj):
    data = {}
    for column in obj.__table__.columns:
        value = getattr(obj, column.name)
        # Convert datetime to string
        if isinstance(value, datetime):
            data[column.name] = value.isoformat()  # Converts to "YYYY-MM-DDTHH:MM:SS"
        else:
            data[column.name] = value
    return data


# lt = ['CE-201', 'CE-215', 'CE-300', 'CE-301']


def getCode_map_status(lt,db):
    corp_coding_data = (
        db.query(model.corp_coding_tab)
        .filter(model.corp_coding_tab.mail_rw_key.in_(lt))
        .all()
    )


    corp_coding_json = json.dumps([to_dict(row) for row in corp_coding_data], indent=4)
    # print(type(corp_coding_json),corp_coding_json)

    corp_coding_data_parsed = json.loads(corp_coding_json)
    cod_re_data = {}
    code_overall = {}
    final_overallStats = {}
    for cod_rw in corp_coding_data_parsed:
        if cod_rw["mail_rw_key"] not in [None,""]:
            if cod_rw["mail_rw_key"] in code_overall:
                code_overall[cod_rw["mail_rw_key"]].extend([{cod_rw["corp_coding_id"]:cod_rw["map_type"]}])
            else:
                code_overall[cod_rw["mail_rw_key"]] = [{cod_rw["corp_coding_id"]:cod_rw["map_type"]}]
            cod_re_data[cod_rw["corp_coding_id"]] = {
                "mail_rw_key": cod_rw["mail_rw_key"],
                "corp_coding_id" : cod_rw["corp_coding_id"],
                "coding_details":cod_rw["coding_details"],
                "invoice_id":cod_rw["invoice_id"],
                "corp_doc_id":cod_rw["corp_doc_id"],
                "approver_name":cod_rw["approver_name"],
                "approver_title": cod_rw["approver_title"],
                "invoicetotal": cod_rw["invoicetotal"],
                "map_type": cod_rw["map_type"],
                "approval_status": cod_rw["approval_status"],


    }

    for rw_ky in code_overall:
        map_status = 1
        for doc_rw in code_overall[rw_ky]:
            if len(list(doc_rw.values()))>0:
                if list(doc_rw.values())[0] == 'Unmapped':
                    map_status = map_status * 1
                else:
                    map_status = map_status + 1

        if map_status==1:
            overall_statusMsg = "Unmapped"
        elif map_status==len(code_overall[rw_ky])+1:
            overall_statusMsg = "Completely Mapped"
        else:
            overall_statusMsg = "Incomplete Mapping"
        final_overallStats[rw_ky] = {"detailed_map_status":code_overall[rw_ky], "overall_status":overall_statusMsg}
        
    return final_overallStats,cod_re_data