from datetime import datetime, timedelta

import jwt
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from passlib.context import CryptContext


class AuthHandler:
    security = HTTPBearer()
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    hash_value = "SECRET"

    def get_password_hash(self, password):
        return self.pwd_context.hash(password)

    def verify_password(self, plain_password, hashed_password):
        return self.pwd_context.verify(plain_password, hashed_password)

    def encode_token(self, user_id):
        payload = {
            "exp": datetime.utcnow() + timedelta(days=0, minutes=360),
            "iat": datetime.utcnow(),
            "sub": user_id,
        }
        return jwt.encode(payload, self.hash_value, algorithm="HS256")

    def decode_token(self, token):
        try:
            payload = jwt.decode(token, self.hash_value, algorithms=["HS256"])
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Signature has expired")
        except jwt.InvalidTokenError as e:
            print(e)
            raise HTTPException(status_code=401, detail="Invalid token")

    def auth_wrapper(self, auth: HTTPAuthorizationCredentials = Security(security)):
        return self.decode_token(auth.credentials)


class OtpHandler:
    security = HTTPBearer()
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    hash_value = "Paasowrd$$$$token@$^%$&"

    def encode_token(self, email_id, otp_code, exp_min=10, user_id=None):
        payload = {
            "exp": datetime.utcnow() + timedelta(days=0, minutes=exp_min),
            "iat": datetime.utcnow(),
            "email": email_id,
            "otp_code": otp_code,
            "user_id": user_id,
        }
        return jwt.encode(payload, self.hash_value, algorithm="HS256")

    def decode_token(self, token):
        try:
            payload = jwt.decode(token, self.hash_value, algorithms=["HS256"])
            return payload["email"], payload["otp_code"], payload["user_id"]
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Signature has expired")
        except jwt.InvalidTokenError as e:
            print(e)
            raise HTTPException(status_code=401, detail="Invalid token")
