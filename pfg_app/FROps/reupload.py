import time

from azure.storage.blob import BlobServiceClient

from pfg_app import settings
from pfg_app.core.utils import get_credential
from pfg_app.FROps.upload import reupload_files_to_azure

ts = str(time.time())
fld_name = ts.replace(".", "_") + "/train"


def del_blobs(cnt_str, cnt_nm, local_path):
    del_cnt = 0
    try:

        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        container_client = blob_service_client.get_container_client(cnt_nm)
        blob_list = container_client.list_blobs(name_starts_with=local_path)

        for blob in blob_list:
            del_cnt = del_cnt + 1
            # later check for delete_blobs status n confirm with status, for
            # now confirming with del_cnt
            container_client.delete_blobs(blob.name)
        if del_cnt > 0:
            del_blobs_status = 1
            del_blobs_msg = str(del_cnt) + " files were deleted."
        else:
            del_blobs_status = 0
            del_blobs_msg = str(del_cnt) + " files were deleted. Error: "

    except Exception as e:
        del_blobs_status = 0
        del_blobs_msg = str(del_cnt) + " files were deleted. Error: " + str(e)
    return del_blobs_status, del_blobs_msg, del_cnt


def reupload_file_azure(
    min_no,
    max_no,
    accepted_file_type,
    file_size_accepted,
    cnt_str,
    cnt_nm,
    local_path,
    old_folder_name,
    upload_type,
):
    blob_fld_name = ""
    try:
        fnl_upload_status, fnl_upload_msg, blob_fld_name = reupload_files_to_azure(
            file_size_accepted,
            accepted_file_type,
            local_path,
            cnt_str,
            cnt_nm,
            old_folder_name,
            upload_type,
        )
        if fnl_upload_status == 1:
            # uploaded!!
            reupload_status = 1
            reupload_status_msg = fnl_upload_msg

        else:
            # no!!
            reupload_status = 0
            reupload_status_msg = fnl_upload_msg

    except Exception as e:
        reupload_status = 0
        reupload_status_msg = str(e)

    return reupload_status, reupload_status_msg, blob_fld_name
