import jwt
import uuid
import typing
from urllib.parse import urlencode, unquote
import hashlib
import hmac

def FUNC_UPBIT_Signature_Generator(
        public_key: str, private_key: str, category: str, params: typing.Dict) -> typing.Dict:
    if (category == 'account') | (category == 'asset') | (category == 'wallet') | (category == 'websocket'):
        payload = {
            'access_key': public_key,
            'nonce': str(uuid.uuid4()),
        }

    elif (category == 'trade') | (category == 'withdraw') | (category == 'deposit'):
        query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")

        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': public_key,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

    jwt_token = jwt.encode(payload, private_key)
    authorization_token = 'Bearer {}'.format(jwt_token)
    header = {"Authorization": authorization_token}

    if category == 'public':
        header = {"accept": "application/json"}

    return header


def FUNC_BINANCE_Signature_Generator(private_key: str, params: typing.Dict) -> str:

    header = hmac.new(
        private_key.encode(),
        urlencode(params).encode(),
        hashlib.sha256).hexdigest()

    return header