import base64
import json
import logging
from typing import Any, Dict, Mapping, Optional

import requests
import rsa
from authlib.jose import JoseError, jwt
from fastapi import HTTPException, Request, status
from fastapi.security import OAuth2AuthorizationCodeBearer

from pfg_app import settings
from pfg_app.azuread.schemas import AzureUser

log = logging.getLogger()


class InvalidAuthorization(HTTPException):
    def __init__(self, detail: Any = None) -> None:
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"},
        )


class AzureADAuthorization(OAuth2AuthorizationCodeBearer):
    # cached AAD jwt keys
    aad_jwt_keys_cache: dict = {}

    def __init__(
        self,
        aad_instance: str = settings.aad_instance,
        aad_tenant: str = settings.aad_tenant_id,
        auto_error: bool = True,
    ):
        self.scopes = ["access_as_user"]
        self.base_auth_url: str = f"{aad_instance}/{aad_tenant}"
        super(AzureADAuthorization, self).__init__(
            authorizationUrl=f"{self.base_auth_url}/oauth2/v2.0/authorize",
            tokenUrl=f"{self.base_auth_url}/oauth2/v2.0/token",
            refreshUrl=f"{self.base_auth_url}/oauth2/v2.0/token",
            scheme_name="oauth2",
            scopes={
                f"api://{settings.api_client_id}/access_as_user": "Access API as user",
            },
            auto_error=auto_error,
        )

    async def __call__(self, request: Request) -> AzureUser:
        token: str = await super(AzureADAuthorization, self).__call__(request) or ""
        self._validate_token_scopes(token)
        decoded_token = self._decode_token(token)
        return self._get_user_from_token(decoded_token)

    @staticmethod
    def _get_user_from_token(decoded_token: Mapping) -> AzureUser:
        try:
            user_id = decoded_token["oid"]
        except Exception as e:
            logging.debug(e)
            raise InvalidAuthorization(
                detail="Unable to extract user details from token"
            )

        return AzureUser(
            id=user_id,
            name=decoded_token.get("name", ""),
            email=decoded_token.get("email", ""),
            preferred_username=decoded_token.get("preferred_username", ""),
            roles=decoded_token.get("roles", []),
        )

    @staticmethod
    def _get_validation_options() -> Dict[str, bool]:
        return {
            "require_aud": True,
            "require_exp": True,
            "require_iss": True,
            "require_iat": True,
            "require_nbf": True,
            "require_sub": True,
            "verify_aud": True,
            "verify_exp": True,
            "verify_iat": True,
            "verify_iss": True,
            "verify_nbf": True,
            "verify_sub": True,
        }

    def _validate_token_scopes(self, token: str):
        """Validate that the requested scopes are in the token's claims."""
        try:
            # Split the JWT into parts (header, payload, signature)
            parts = token.split(".")
            if len(parts) != 3:
                raise InvalidAuthorization("Malformed token received")

            # Decode the payload (the second part of the JWT)
            payload_segment = parts[1]
            padded_payload = payload_segment + "=" * (
                4 - len(payload_segment) % 4
            )  # Add padding
            decoded_bytes = base64.urlsafe_b64decode(padded_payload.encode("utf-8"))
            claims = json.loads(decoded_bytes.decode("utf-8"))

        except (ValueError, json.JSONDecodeError) as e:
            log.debug(f"Malformed token: {token}, {e}")
            raise InvalidAuthorization("Malformed token received")

        try:
            token_scopes = claims.get("scp", "").split(" ")
        except Exception:
            log.debug("Malformed scopes")
            raise InvalidAuthorization("Malformed scopes")

        for scope in self.scopes:
            if scope not in token_scopes:
                raise InvalidAuthorization("Missing a required scope")

    @staticmethod
    def _get_key_id(token: str) -> Optional[str]:
        """Decode the JWT header without verifying the signature."""
        try:
            # Split the token into its components
            header_segment = token.split(".")[0]
            # Add padding if necessary and decode the base64-encoded header
            padded_header = header_segment + "=" * (4 - len(header_segment) % 4)
            decoded_bytes = base64.urlsafe_b64decode(padded_header.encode("utf-8"))
            headers = json.loads(decoded_bytes.decode("utf-8"))
            return headers["kid"] if headers and "kid" in headers else None
        except Exception as e:
            raise InvalidAuthorization(f"Malformed token: {token}, {e}")

    @staticmethod
    def _ensure_b64padding(key: str) -> str:
        """The base64 encoded keys are not always correctly padded, so pad with
        the right number of ="""
        # key = key.encode('utf-8') #TODO check if this is needed
        missing_padding = len(key) % 4
        for _ in range(missing_padding):
            # key = key + b'=' #TODO check if this is correct or bottom one
            key += "=" * (4 - missing_padding)
        return key

    def _cache_aad_keys(self) -> None:
        """
        Cache all AAD JWT keys - so we don't have to make a web call each auth request
        """
        response = requests.get(
            f"{self.base_auth_url}/v2.0/.well-known/openid-configuration",
            timeout=60,
        )
        aad_metadata = response.json() if response.ok else None
        jwks_uri = (
            aad_metadata["jwks_uri"]
            if aad_metadata and "jwks_uri" in aad_metadata
            else None
        )
        if jwks_uri:
            response = requests.get(jwks_uri, timeout=60)
            keys = response.json() if response.ok else None
            if keys and "keys" in keys:
                for key in keys["keys"]:
                    n = int.from_bytes(
                        base64.urlsafe_b64decode(self._ensure_b64padding(key["n"])),
                        "big",
                    )
                    e = int.from_bytes(
                        base64.urlsafe_b64decode(self._ensure_b64padding(key["e"])),
                        "big",
                    )
                    pub_key = rsa.PublicKey(n, e)
                    # Cache the PEM formatted public key.
                    AzureADAuthorization.aad_jwt_keys_cache[key["kid"]] = (
                        pub_key.save_pkcs1()
                    )

    def _get_token_key(self, key_id: str) -> str:
        if key_id not in AzureADAuthorization.aad_jwt_keys_cache:
            self._cache_aad_keys()
        return AzureADAuthorization.aad_jwt_keys_cache[key_id]

    def _decode_token(self, token: str) -> Mapping:
        key_id = self._get_key_id(token)
        if not key_id:
            raise InvalidAuthorization("The token does not contain kid")

        key = self._get_token_key(key_id)
        try:
            # No "token" keyword arg; pass token and key directly
            decoded_token = jwt.decode(
                token, key, claims_cls=None
            )  # 'claims_cls' is optional
            # Validate audience (if necessary,
            # as authlib does not have explicit 'audience' handling)
            if decoded_token.get("aud") != settings.api_audience:
                if "api://"+decoded_token.get("aud") != settings.api_audience:
                    raise InvalidAuthorization("Invalid audience")
            return decoded_token

        except JoseError as e:
            logging.debug(f"Token decoding error: {e}")
            raise InvalidAuthorization("The token is invalid")
        except Exception as e:
            logging.debug(f"Unexpected error: {e}")
            raise InvalidAuthorization("Unable to decode token")


authorize = AzureADAuthorization()
