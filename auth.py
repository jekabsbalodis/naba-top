import time
from typing import Final

from authlib.integrations.starlette_client import OAuth, OAuthError
from nicegui import app
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse

from config import config

UNRESTRICTED_ROUTES: Final[list[str]] = [
    '/',
    '/login',
    '/auth',
    '/logout',
]


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if (
            path in UNRESTRICTED_ROUTES
            or path.startswith('/_nicegui')
            or path.startswith('/static')
        ):
            return await call_next(request)

        user = request.session.get('user_info')

        if not user or not _is_valid(user):
            request.session.pop('user', None)
            return RedirectResponse(f'/login?redirect_to={request.url.path}')

        return await call_next(request)


oauth = OAuth()


def _is_valid(user: dict) -> bool:
    if not user:
        return False

    try:
        exp = user.get('exp')
        if not exp or int(exp) <= int(time.time()):
            return False

        aud = user.get('aud')
        if aud != config.CLIENT_ID:
            return False

        iss = user.get('iss')
        if iss and not iss == config.EXPECTED_ISSUER:
            return False

        return True

    except ValueError, TypeError:
        return False


def setup_auth():
    """Initialize authentication middleware and routes"""
    oauth.register(
        'pocketid',
        client_id=config.CLIENT_ID,
        client_secret=None,
        server_metadata_url=config.SERVER_METADATA_URL,
        client_kwargs={
            'scope': 'openid email profile',
            'code_challenge_method': 'S256',
        },
    )

    app.add_middleware(SessionMiddleware, secret_key=config.SESSION_SECRET)  # ty:ignore[invalid-argument-type]
    app.add_middleware(AuthMiddleware)  # ty:ignore[invalid-argument-type]

    @app.get('/login')
    async def login(request: Request):
        redirect_uri = config.CALLBACK_URL
        return await oauth.pocketid.authorize_redirect(request, redirect_uri)

    @app.get('/auth')
    async def auth(request: Request):
        try:
            token = await oauth.pocketid.authorize_access_token(request)
        except OAuthError as e:
            return RedirectResponse(url=f'/?error={str(e)}')

        user = token.get('userinfo')
        if user:
            request.session['user'] = dict(user)
        return RedirectResponse(url='/')

    @app.get('/logout')
    async def logout(request: Request):
        request.session.clear()
        return RedirectResponse(url='/')
