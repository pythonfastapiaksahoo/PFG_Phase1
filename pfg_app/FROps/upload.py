import math
import os
import traceback

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlobServiceClient, PartialBatchErrorException

credential = DefaultAzureCredential()
fnl_upload_status = 0
fnl_upload_msg = ""

# 1. check Number of files:


def no_of_files(min_no, max_no, connection_str, containername, dir_path):
    local_upld_cnt = 0
    try:
        account_name = connection_str.split("AccountName=")[1].split(";AccountKey")[0]
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )
        container_client = blob_service_client.get_container_client(containername)
        blob_list = container_client.list_blobs(name_starts_with=dir_path + "/")
        filenames = []
        for b in blob_list:
            filenames.append(b.name)
        local_upld_cnt = len(filenames)
        if min_no <= local_upld_cnt <= max_no:
            no_of_files_status = 1
            no_of_files_msg = "Good to go at no_of_files"
        elif min_no > local_upld_cnt:
            no_of_files_status = 0
            no_of_files_msg = (
                "Minimum " + str(min_no) + " files are required for training!"
            )
        elif local_upld_cnt > max_no:
            no_of_files_status = 0
            no_of_files_msg = (
                "Sorry, you can not upload files more than " + str(max_no) + "."
            )
        else:
            no_of_files_status = 0
            no_of_files_msg = (
                "Please ensure, the uploaded directory has a minimum of "
                + str(min_no)
                + " to maximum of "
                + str(max_no)
                + " files."
            )
    except Exception as e:
        no_of_files_status = 0
        no_of_files_msg = str(e)
    return no_of_files_status, no_of_files_msg, local_upld_cnt


# 2. check file type:
def get_extn(st):
    return st.split(".")[-1].lower()


def check_filetype(dir_path, connection_str, containername, accepted_file_type):
    try:
        account_name = connection_str.split("AccountName=")[1].split(";AccountKey")[0]
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )
        container_client = blob_service_client.get_container_client(containername)
        blob_list = container_client.list_blobs(name_starts_with=dir_path + "/")
        filenames = []
        for b in blob_list:
            filenames.append(b.name)
        extn = list(map(get_extn, filenames))
        present_file_type = list(set(extn))
        not_accepted = set(present_file_type) - set(accepted_file_type)
        print(not_accepted)
        if len(not_accepted) == 0:
            check_filetype_status = 1
            check_filetype_msg = "Good to go"
        else:
            check_filetype_status = 0
            check_filetype_msg = (
                "Please check the uploaded file type, Accepted types: "
                + str(accepted_file_type)
            )
    except Exception as e:
        check_filetype_status = 0
        check_filetype_msg = str(e)
    return check_filetype_status, check_filetype_msg


# 3. check file size:


def ck_size_limit(fld_path, connection_str, containername, file_size_accepted):
    fl_sts_msg = ""
    fl_status = 1
    try:
        account_name = connection_str.split("AccountName=")[1].split(";AccountKey")[0]
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )
        container_client = blob_service_client.get_container_client(containername)
        blob_list = container_client.list_blobs(name_starts_with=fld_path + "/")
        for b in blob_list:
            size_bytes = b.size

            if size_bytes == 0:
                # fl_sts_msg = "0 byte file found, Please upload valid files"
                # print("0000000")
                fl_status = fl_status * 0
            elif size_bytes > 0:  # min os ok
                # size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
                i = int(math.floor(math.log(size_bytes, 1024)))
                # print(i)
                if i == 2:
                    p = math.pow(1024, i)
                    s = round(size_bytes / p, 2)
                    # print(s)
                    if s >= file_size_accepted:
                        fl_status = fl_status * 0
                    else:
                        fl_status = fl_status * 2
                elif i < 2:
                    fl_status = fl_status * 2

        if fl_status == 0:
            fl_sts_msg = "Please check the file size. "
            +"Uploaded flies should be grater than 0 Bytes and be less than 50 MB"
        elif fl_status > 1:
            fl_sts_msg = "Good to upload"
            fl_status = 1
        else:
            fl_sts_msg = "Please check the files"
            fl_status = 0
    except Exception as e:
        fl_sts_msg = str(e)
        fl_status = 0
    return fl_status, fl_sts_msg


def upload_blobs(cnt_str, cnt_nm, local_path, old_fld_name):
    upload_blobs_status = 0
    upload_blobs_msg = "Issue at upload_blobs"
    blob_fld_name = old_fld_name
    account_name = cnt_str.split("AccountName=")[1].split(";AccountKey")[0]
    account_url = f"https://{account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(
        account_url=account_url, credential=credential
    )
    container_client = blob_service_client.get_container_client(cnt_nm)
    blob_list = container_client.list_blobs(name_starts_with=local_path + "/")
    upld_cnt = 0
    try:
        #       container_client.create_container()
        for file in blob_list:
            file_path_on_azure = os.path.join(old_fld_name, file.name.split("/")[-1])
            # BlobClient.create_blob_from_path(container_name,file_path_on_azure,file_path_on_local)
            blob = BlobClient.from_connection_string(
                conn_str=cnt_str, container_name=cnt_nm, blob_name=file.name
            )
            data = blob.download_blob().readall()
            container_client.upload_blob(name=file_path_on_azure, data=data)
            upld_cnt = upld_cnt + 1
        upload_blobs_status = 1
        upload_blobs_msg = "Files Uploaded"
    except Exception as e:
        print(e)
        upload_blobs_status = 0
        upload_blobs_msg = str(e)
    return upload_blobs_status, upload_blobs_msg, upld_cnt, blob_fld_name


# check upload confirm:


def upload_confirm(cnt_str, cnt_nm, blb_fldr):
    try:
        upload_confirm_status = 1
        upload_confirm_msg = "All files uploaded"
    except Exception as e:
        upload_confirm_status = 0
        upload_confirm_msg = (
            "Files are not uploaded,Please try again after sometime. (" + str(e) + ")"
        )

    return upload_confirm_status, upload_confirm_msg


def del_blobs(cnt_str, cnt_nm, blob_prefix):
    del_cnt = 0
    try:
        account_name = cnt_str.split("AccountName=")[1].split(";AccountKey")[0]
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )
        container_client = blob_service_client.get_container_client(cnt_nm)

        # Print the prefix for debugging
        print(f"Deleting blobs with prefix: {blob_prefix}")
        blob_list = container_client.list_blobs(name_starts_with=blob_prefix)

        # Collect blob names to delete all at once
        blobs_to_delete = [blob.name for blob in blob_list]

        if blobs_to_delete:
            # Print the names of blobs being deleted (for debugging)
            print(f"Deleting blobs: {blobs_to_delete}")
            container_client.delete_blobs(
                *blobs_to_delete
            )  # delete multiple blobs at once
            del_cnt = len(blobs_to_delete)
            del_blobs_status = 1
            del_blobs_msg = f"{del_cnt} files were deleted."
        else:
            del_blobs_status = 0
            del_blobs_msg = "No files were found to delete."

    except PartialBatchErrorException as e:
        # Handle partial deletion errors gracefully
        del_blobs_status = 0
        del_blobs_msg = f"Partial failure during blob deletion: {e}"
    except Exception as e:
        del_blobs_status = 0
        del_blobs_msg = f"{del_cnt} files were deleted. Error: {str(e)}"
    return del_blobs_status, del_blobs_msg, del_cnt


# Upload click:


def reupload_files_to_azure(
    file_size_accepted,
    accepted_file_type,
    local_path,
    cnt_str,
    cnt_nm,
    old_folder_name,
    upload_type,
):
    try:
        blob_fld_name = old_folder_name
        check_filetype_status, check_filetype_msg = check_filetype(
            local_path, cnt_str, cnt_nm, accepted_file_type
        )
        if check_filetype_status == 1:
            # check file size limitation:
            fl_status, fl_sts_msg = ck_size_limit(
                local_path, cnt_str, cnt_nm, file_size_accepted
            )

            if fl_status == 1:
                # Upload blobs:
                if upload_type == "Fresh":
                    del_blobs(cnt_str, cnt_nm, old_folder_name)
                upload_blobs_status, upload_blobs_msg, upld_cnt, blob_fld_name = (
                    upload_blobs(cnt_str, cnt_nm, local_path, old_folder_name)
                )
                upload_confirm_status, upload_confirm_msg = upload_confirm(
                    cnt_str, cnt_nm, old_folder_name
                )
                fnl_upload_status = 1
                fnl_upload_msg = "Files upload successful"

            else:
                fnl_upload_status = 0
                fnl_upload_msg = fl_sts_msg
        else:
            fnl_upload_status = 0
            fnl_upload_msg = check_filetype_msg
        del_blobs_status, del_blobs_msg, del_cnt = del_blobs(
            cnt_str, cnt_nm, local_path + "/"
        )
    except Exception as e:
        print(
            f"Error in reupload_files_to_azure func() line 250: {e}",
            traceback.format_exc(),
        )
    return fnl_upload_status, fnl_upload_msg, blob_fld_name


def upload_files_to_azure(
    min_no,
    max_no,
    accepted_file_type,
    file_size_accepted,
    cnt_str,
    cnt_nm,
    local_path,
    old_folder_name,
):
    global fld_name
    blob_fld_name = ""
    try:
        # Check no of files limitation:
        no_of_files_status, no_of_files_msg, local_upld_cnt = no_of_files(
            min_no, max_no, cnt_str, cnt_nm, local_path
        )
        if no_of_files_status == 1:
            # Check file type limitation:
            check_filetype_status, check_filetype_msg = check_filetype(
                local_path, cnt_str, cnt_nm, accepted_file_type
            )

            if check_filetype_status == 1:
                # check file size limitation:
                fl_status, fl_sts_msg = ck_size_limit(
                    local_path, cnt_str, cnt_nm, file_size_accepted
                )

                if fl_status == 1:
                    # Upload blobs:
                    del_blobs_status, del_blobs_msg, del_cnt = del_blobs(
                        cnt_str, cnt_nm, old_folder_name
                    )
                    upload_blobs_status, upload_blobs_msg, upld_cnt, blob_fld_name = (
                        upload_blobs(cnt_str, cnt_nm, local_path, old_folder_name)
                    )
                    upload_confirm_status, upload_confirm_msg = upload_confirm(
                        cnt_str, cnt_nm, old_folder_name
                    )
                    fnl_upload_status = 1
                    fnl_upload_msg = "Files upload successful"

                else:
                    fnl_upload_status = 0
                    fnl_upload_msg = fl_sts_msg
            else:
                fnl_upload_status = 0
                fnl_upload_msg = check_filetype_msg
        else:
            fnl_upload_status = 0
            fnl_upload_msg = no_of_files_msg
        del_blobs_status, del_blobs_msg, del_cnt = del_blobs(
            cnt_str, cnt_nm, local_path + "/"
        )
    except Exception as e:
        fnl_upload_status = 0
        fnl_upload_msg = str(e)
    return fnl_upload_status, fnl_upload_msg, blob_fld_name
