import json

from authlib.integrations.starlette_client import OAuth, OAuthError
from nicegui import app, ui
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse

oauth = OAuth()

oauth.register(
    'pocketid',
    client_id='6a80f266-c20c-45f6-943e-888dd15359ce',
    client_secret=None,
    server_metadata_url='https://auth.balodis.id.lv/.well-known/openid-configuration',
    client_kwargs={
        'scope': 'openid email profile',
        'code_challenge_method': 'S256',
    },
)

app.add_middleware(SessionMiddleware, secret_key='secret')  # ty:ignore[invalid-argument-type]


@ui.page('/')
async def homepage(request: Request):
    user = request.session.get('user')
    if user:
        data = json.dumps(user)
        ui.code(data)
    else:
        ui.label('hello')


@app.get('/login')
async def login(request: Request):
    redirect_uri = request.url_for('auth')
    return await oauth.pocketid.authorize_redirect(request, redirect_uri)


@app.get('/auth')
async def auth(request: Request):
    try:
        token = await oauth.pocketid.authorize_access_token(request)
    except OAuthError as e:
        return e
    user = token.get('userinfo')
    if user:
        request.session['user'] = dict(user)
    return RedirectResponse(url='/')


@app.get('/logout')
async def logout(request: Request):
    request.session.pop('user', None)
    return RedirectResponse(url='/')


ui.run(host='localhost', storage_secret='secrets')
