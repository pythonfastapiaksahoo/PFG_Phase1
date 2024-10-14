from fastapi import APIRouter, Depends, Response

# from pfg_app import settings
from pfg_app.azuread.auth import get_admin_user

# from pfg_app.core import azure_fr as core_fr
from pfg_app.core.utils import get_blob_securely

router = APIRouter(
    prefix="/apiv1.1/Common",
    tags=["Common"],
    dependencies=[Depends(get_admin_user)],
    responses={404: {"description": "Not found"}},
)


@router.get("/get-blob-file")
def get_blob_file(container_name: str, blob_path: str):
    """API route to retrieve a file from Azure Blob Storage.

    Parameters:
    ----------
    file_name : str
        Name of the file to retrieve from Azure Blob Storage.

    Returns:
    -------
    File object from Azure Blob Storage.
    """

    blob_name = blob_path.split("/")[-1]

    blob_data, content_type = get_blob_securely(container_name, blob_path)

    headers = {
        "Content-Disposition": f"inline; filename={blob_name}",
        "Content-Type": content_type,
    }
    return Response(content=blob_data, headers=headers, media_type=content_type)


# @router.get("/call-azure-document-intelligence")
# def call_azure_document_intelligence(container_name: str, blob_path: str):
#     """API route to call the Azure Document Intelligence API.

#     Returns:
#     -------
#     Response from the Azure Document Intelligence API.
#     """
#     file_data, content_type = get_blob_securely(
#         container_name=container_name, blob_path=blob_path
#     )
#     response = core_fr.call_form_recognizer(
#         file_data=file_data,
#         endpoint=settings.form_recognizer_endpoint,
#         api_version=settings.api_version,
#     )
#     return response
