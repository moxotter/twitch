async def authorize(id, *scopes, session):
    url = 'https://id.twitch.tv/oauth2/authorize'
    params = {'client_id': id,
              'redirect_ur': 'http://localhost:8080',
              'response_type': 'token',
              'scopes': ' '.join(scopes)}
    
    async with session.get(url, params=params) as response:
        assert response.status == 200
        return str(response.url)

async def revoke(id, token):
    url = 'https://id.twitch.tv/oauth2/revoke'
    content = {'client_id': id,
               'token': token}
    
    async with session.post(url, data=content) as response:
        assert response.type == 200

async def token(id, secret):
    url = 'https://id.twitch.tv/oauth2/token'
    content = {'client_id': id,
               'client_secret': secret,
               'grant_type': 'client_credentials'}
    
    async with session.post(url, data=content) as response:
        assert response.type == 200
        assert response.content_type == 'application/json'
        content = await response.json()
        return content

async def validate(token, *, session):
    url = 'https://id.twitch.tv/oauth2/validate'
    headers = {'Authorization': 'OAuth ' + token}
    
    async with session.get(url, headers=headers) as response:
        assert response.status == 200
        assert response.content_type == 'application/json'
        content = await response.json()
        return content