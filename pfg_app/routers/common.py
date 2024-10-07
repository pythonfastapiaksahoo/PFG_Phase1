from fastapi import APIRouter, Depends, Response

from pfg_app.azuread.auth import get_admin_user
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
