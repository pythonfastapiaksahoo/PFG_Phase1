# import json
# import os
# import sys
# import traceback

# import jwt
# import model
# import requests
# from auth import AuthHandler
# from azure.identity import DefaultAzureCredential
# from azure.storage.blob import BlobServiceClient
# from fastapi import APIRouter, Depends, Request, Response, status
# from session import get_db
# from sqlalchemy.orm import Load, Session

# sys.path.append("..")

# credential = DefaultAzureCredential()
# # model.Base.metadata.create_all(bind=engine)
# auth_handler = AuthHandler()

# router = APIRouter(
#     prefix="/apiv1.1/emailconfig",
#     tags=["EmailListener"],
#     dependencies=[Depends(auth_handler.auth_wrapper)],
#     responses={404: {"description": "Not found"}},
# )


# def getOcrParameters(customerID, db):
#     try:
#         configs = (
#             db.query(model.FRConfiguration)
#             .filter(model.FRConfiguration.idCustomer == customerID)
#             .first()
#         )
#         return configs
#     except Exception as e:
#         return Response(
#             status_code=500, headers={"DB Error": "Failed to get OCR parameters"}
#         )
#     finally:
#         db.close()


# def getuserdetails(username, db):
#     """### Function to fetch the details of the user performing log in :param
#     username: It is a string parameter for providing username :param db: It
#     provides a session to interact with the backend Database,that is of Session
#     Object Type.

#     :return: It returns the user data
#     """
#     try:
#         data = (
#             db.query(
#                 model.User,
#                 model.Credentials,
#                 model.AccessPermission.idAccessPermission,
#                 model.AccessPermissionDef,
#                 model.AmountApproveLevel,
#             )
#             .options(
#                 Load(model.User).load_only("firstName", "lastName", "isActive"),
#                 Load(model.AccessPermissionDef).load_only(
#                     "NameOfRole",
#                     "Priority",
#                     "User",
#                     "Permissions",
#                     "isUserRole",
#                     "AccessPermissionTypeId",
#                     "allowBatchTrigger",
#                     "allowServiceTrigger",
#                     "NewInvoice",
#                 ),
#                 Load(model.Credentials).load_only("LogSecret", "crentialTypeId"),
#             )
#             .filter(model.User.idUser == model.Credentials.userID)
#             .filter(model.Credentials.crentialTypeId.in_((1, 2)))
#             .filter(model.Credentials.LogName == username)
#             .join(
#                 model.AccessPermission,
#                 model.AccessPermission.userID == model.User.idUser,
#                 isouter=True,
#             )
#             .join(
#                 model.AccessPermissionDef,
#                 model.AccessPermissionDef.idAccessPermissionDef
#                 == model.AccessPermission.permissionDefID,
#             )
#             .join(
#                 model.AmountApproveLevel,
#                 model.AmountApproveLevel.idAmountApproveLevel
#                 == model.AccessPermissionDef.amountApprovalID,
#                 isouter=True,
#             )
#         )
#         return data.all()
#     except Exception as e:
#         print(traceback.print_exc())
#     finally:
#         db.close()


# @router.get("/getemailconfig", status_code=status.HTTP_200_OK)
# async def getemailconfig(db: Session = Depends(get_db)):
#     try:
#         configs = getOcrParameters(1, db)
#         message = "success"
#         config = {
#             "basicauth": False,
#             "email": "",
#             "email_tenant_id": "",
#             "email_client_secret": "",
#             "email_client_id": "",
#             "host": "",
#             "folder": "",
#             "acceptedEmails": "",
#             "acceptedDomains": "",
#             "accepted_attachments": [
#                 "application/pdf",
#                 "image/png",
#                 "image/jpg",
#                 "image/jpeg",
#                 "text/html",
#             ],
#             "bulk_attachments": [
#                 "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#             ],
#             "connectStr": configs.ConnectionString,
#             "containerName": configs.ContainerName,
#             "accountname": configs.ConnectionString.split("AccountName=")[1].split(
#                 ";AccountKey="
#             )[0],
#             "accountkey": configs.ConnectionString.split("AccountKey=")[1].split(
#                 ";EndpointSuffix"
#             )[0],
#             "host-endpoint": "",
#             "accepted_headers": "document url",
#             "loginuser": "",
#             "loginpass": "",
#             "bulk_upload_url": "",
#             "serina-endpoint": configs.Endpoint,
#             "model": "prebuilt-invoice",
#             "fr-version": "api-version=2022-08-31",
#             "fr-key": configs.Key1,
#             "synonyms": createsynonym_config(db, None),
#         }
#         return {"message": message, "config": config}
#     except Exception as e:
#         return {"message": f"exception {e}", "config": {}}
#     finally:
#         db.close()


# @router.get("/getVendorSynonymsByEntity/{entityID}", status_code=status.HTTP_200_OK)
# def getsynonym_byentity(entityID: int, db: Session = Depends(get_db)):
#     return createsynonym_config(db, entityID)


# @router.post("/saveemailconfig", status_code=status.HTTP_200_OK)
# async def saveemailconfig(request: Request, db: Session = Depends(get_db)):
#     try:
#         req_body = await request.json()
#         email = req_body["email"]
#         client_id = req_body["email_client_id"]
#         client_secret = req_body["email_client_secret"]
#         tenant = req_body["email_tenant_id"]
#         folder = req_body["folder"]
#         loginuser = req_body["loginuser"]
#         loginpass = req_body["loginpass"]
#         host = req_body["host"]
#         acceptedDomains = req_body["acceptedDomains"]
#         acceptedEmails = req_body["acceptedEmails"]
#         userdetails = getuserdetails(loginuser, db)
#         if not auth_handler.verify_password(loginpass, userdetails[0][1].LogSecret):
#             return {"message": f"Invalid username and/or password"}
#         tenant_id = os.getenv("SERVICE_TENANT_ID")
#         token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
#         body = {
#             "grant_type": "client_credentials",
#             "client_id": os.getenv("SERVICE_PRINCIPAL"),
#             "scope": "https://vault.azure.net/.default",
#             "client_secret": os.getenv("SERVICE_CLIENT_SECRET"),
#         }
#         token_resp = requests.post(
#             token_url,
#             data=body,
#             headers={"Content-Type": "application/x-www-form-urlencoded"},
#         )
#         access_token = token_resp.json()["access_token"]
#         keyvault_url = os.getenv("KEYVAULT")
#         secret_name = os.getenv("KV_SECRET3")
#         vault_url = f"{keyvault_url}/secrets/{secret_name}?api-version=7.2"
#         configresp = requests.get(
#             vault_url, headers={"Authorization": f"Bearer {access_token}"}
#         )
#         if configresp.status_code == 200:
#             config = configresp.json()["value"]
#             final_dict = jwt.decode(config, auth_handler.secret, algorithms=["HS256"])
#             final_dict["email"] = email
#             final_dict["email_tenant_id"] = tenant
#             final_dict["email_client_id"] = client_id
#             final_dict["email_client_secret"] = client_secret
#             final_dict["folder"] = folder
#             final_dict["host"] = host
#             final_dict["loginuser"] = loginuser
#             final_dict["loginpass"] = loginpass
#             final_dict["acceptedDomains"] = acceptedDomains
#             final_dict["acceptedEmails"] = acceptedEmails
#         else:
#             configs = getOcrParameters(1, db)
#             connection_str = configs.ConnectionString
#             containername = configs.ContainerName
#             account_name = connection_str.split("AccountName=")[1].split(";AccountKey")[
#                 0
#             ]
#             account_url = f"https://{account_name}.blob.core.windows.net"
#             blob_service_client = BlobServiceClient(
#                 account_url=account_url, credential=credential
#             )
#             account_key = connection_str.split("AccountKey=")[1].split(
#                 ";EndpointSuffix"
#             )[0]
#             final_dict = {
#                 "email": email,
#                 "email_tenant_id": tenant,
#                 "email_client_id": client_id,
#                 "email_client_secret": client_secret,
#                 "host": host,
#                 "folder": folder,
#                 "acceptedEmails": acceptedEmails,
#                 "acceptedDomains": acceptedDomains,
#                 "accepted_attachments": [
#                     "application/pdf",
#                     "image/png",
#                     "image/jpg",
#                     "image/jpeg",
#                 ],
#                 "bulk_attachments": [
#                     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#                 ],
#                 "connectStr": connection_str,
#                 "containerName": containername,
#                 "accountname": blob_service_client.account_name,
#                 "accountkey": account_key,
#                 "host-endpoint": "https://serina-qa.centralindia.cloudapp.azure.com/apiv1.1",
#                 "accepted_headers": "document url",
#                 "loginuser": loginuser,
#                 "loginpass": loginpass,
#                 "bulk_upload_url": "http://20.204.220.145",
#                 "serina-endpoint": configs.Endpoint,
#                 "model": "prebuilt-invoice",
#                 "fr-version": "api-version=2022-08-31",
#                 "fr-key": configs.Key1,
#             }
#         encoded_dict = jwt.encode(final_dict, auth_handler.secret, algorithm="HS256")
#         set_body = {"value": encoded_dict}
#         setresp = requests.put(
#             vault_url,
#             data=json.dumps(set_body),
#             headers={
#                 "Content-Type": "application/json",
#                 "Authorization": f"Bearer {access_token}",
#             },
#         )
#         mes = "secret created"
#         return {"message": "success", "details": mes}
#     except Exception as e:
#         return {"message": f"exception", "details": e}
#     finally:
#         db.close()


# def createsynonym_config(db, entityID):
#     try:
#         if entityID:
#             Synonyms = (
#                 db.query(model.Vendor)
#                 .filter(
#                     model.Vendor.Synonyms.isnot(None), model.Vendor.entityID == entityID
#                 )
#                 .all()
#             )
#         else:
#             Synonyms = (
#                 db.query(model.Vendor).filter(model.Vendor.Synonyms.isnot(None)).all()
#             )
#         obj = {}
#         for s in Synonyms:
#             obj[s.VendorName] = json.loads(s.Synonyms, strict=False)
#         return obj
#     except Exception as e:
#         print(e)
#         return {}
