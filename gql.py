import base64
import json

def parse_cursor(cursor):
    cursor = base64.b64decode(cursor.encode()).decode()
    cursor = json.loads(cursor)
    return cursor

def build_cursor(cursor):
    cursor = json.dumps(cursor)
    cursor = base64.b64encode(cursor.encode()).decode()
    return cursor

def build_cursor_followers(id, dt):
    cursor = {'tp': ' ',
              'ts': f'user:{id}',
              'ip': f'user:{id}:followed_by',
              'is': str(int(dt.timestamp() * 1000000000))}
    cursor = build_cursor(cursor)
    return cursor

def build_cursor_follows(id, dt):
    cursor = {'tp': f'user:{id}:follows',
              'ts': ' ',
              'ip': f'user:{id}:follows',
              'is': str(int(dt.timestamp() * 1000000000))}
    cursor = build_cursor(cursor)
    return cursor

def build_cursor_mods(id, dt):
    cursor = {'tp': ' ',
              'ts': f'channel:{id}',
              'ip': f'channel:{id}:moderated_by',
              'is': str(int(dt.timestamp() * 1000000000))}
    cursor = build_cursor(cursor)
    return cursor

def build_cursor_vips(id, dt):
    cursor = {'tp': ' ',
              'ts': f'user:{id}',
              'ip': f'user:{id}:has_vip',
              'is': str(int(dt.timestamp() * 1000000000))}
    cursor = build_cursor(cursor)
    return cursor

def build_cursor_streams(dt):
    cursor = {'s': dt.timestamp(), 'd': False, 't': True}
    cursor = build_cursor(cursor)
    return cursor

async def query(fmt, *args, session, token=None):
    url = 'https://gql.twitch.tv/gql'
    headers = {'Client-Id': 'kimne78kx3ncx6brgo4mv6wki5h1ko'}
    content = {'query': fmt % args}
    
    if token:
        headers['Authorization'] = 'OAuth ' + token
    
    async with session.post(url, headers=headers, json=content) as response:
        assert response.status == 200
        assert response.content_type == 'application/json'
        content = await response.json()
        if 'errors' in content:
            message = ' '.join(error['message'] for error in content['errors'])
            raise Exception(message)
        return content

async def query_user(id=None, login=None, fields=None, *, session):
    assert id or login
    
    fields = fields or ['id', 'login']
    
    fmt = 'query{user(%s){%s}}'
    arg1 = f'id:{id}' if id else f'login:"{login}"'
    arg2 = ','.join(fields)
    args = (arg1, arg2)
    
    content = await query(fmt, *args, session=session)
    
    return content['data']['user']

async def query_users(ids=None, logins=None, fields=None, *, session):
    assert ids or logins
    
    if ids:
        ids = ','.join(str(id) for id in ids)
    
    if logins:
        logins = ','.join(f'"{login}"' for login in logins)
    
    fields = fields or ['id', 'login']
    
    fmt = 'query{users(%s){%s}}'
    arg1 = f'ids:[{ids}]' if ids else f'logins:[{logins}]'
    arg2 = ','.join(fields)
    args = (arg1, arg2)
    
    content = await query(fmt, *args, session=session)
    
    for user in content['data']['users']:
        yield user

async def query_user_followers(id=None, login=None, fields=None, *, cursor=None, session):
    assert id or login
    
    fields = fields or ['id', 'login']
    cursor = cursor or ''
    
    fmt = 'query{user(%s){followers(after:"%s",first:100){edges{cursor,followedAt,node{%s}}}}}'
    arg1 = f'id:{id}' if id else f'login:"{login}"'
    arg3 = ','.join(fields)
    
    while True:
        arg2 = cursor
        args = (arg1, arg2, arg3)
        
        content = await query(fmt, *args, session=session)
        
        if not content['data']['user']['followers']['edges']:
            break
        
        for edge in content['data']['user']['followers']['edges']:
            cursor = edge['cursor']
            yield edge['followedAt'], edge['node']
        
        if not cursor:
            break

async def query_user_followers_count(id=None, login=None, *, session):
    assert id or login
    
    fmt = 'query{user(%s){followers{totalCount}}}'
    arg = f'id:{id}' if id else f'login:"{login}"'
    
    content = await query(fmt, arg, session=session)
    
    return content['user']['followers']['totalCount']

async def query_user_follows(id=None, login=None, fields=None, *, cursor=None, session):
    assert id or login
    
    fields = fields or ['id', 'login']
    cursor = cursor or ''
    
    fmt = 'query{user(%s){follows(after:"%s",first:100){edges{cursor,followedAt,node{%s}}}}}'
    arg1 = f'id:{id}' if id else f'login:"{login}"'
    arg3 = ','.join(fields)
    
    while True:
        arg2 = cursor
        args = (arg1, arg2, arg3)
        
        content = await query(fmt, *args, session=session)
        
        if not content['data']['user']['follows']['edges']:
            break
        
        for edge in content['data']['user']['follows']['edges']:
            cursor = edge['cursor']
            yield edge['followedAt'], edge['node']
        
        if not cursor:
            break

async def query_user_follows_count(id=None, login=None, *, session):
    assert id or login
    
    fmt = 'query{user(%s){follows{totalCount}}}'
    arg = f'id:{id}' if id else f'login:"{login}"'
    
    content = await query(fmt, arg, session=session)
    
    return content['user']['follows']['totalCount']

async def query_user_mods(id=None, login=None, fields=None, *, cursor=None, session):
    assert id or login
    
    fields = fields or ['id', 'login']
    cursor = cursor or ''
    
    fmt = 'query{user(%s){mods(after:"%s",first:100){edges{cursor,grantedAt,node{%s}}}}}'
    arg1 = f'id:{id}' if id else f'login:"{login}"'
    arg3 = ','.join(fields)
    
    while True:
        arg2 = cursor
        args = (arg1, arg2, arg3)
        
        content = await query(fmt, *args, session=session)
        
        if not content['data']['user']['mods']['edges']:
            break
        
        for edge in content['data']['user']['mods']['edges']:
            cursor = edge['cursor']
            yield edge['grantedAt'], edge['node']
        
        if not cursor:
            break

async def query_user_vips(id=None, login=None, fields=None, *, cursor=None, session):
    assert id or login
    
    fields = fields or ['id', 'login']
    cursor = cursor or ''
    
    fmt = 'query{user(%s){vips(after:"%s",first:100){edges{cursor,grantedAt,node{%s}}}}}'
    arg1 = f'id:{id}' if id else f'login:"{login}"'
    arg3 = ','.join(fields)
    
    while True:
        arg2 = cursor
        args = (arg1, arg2, arg3)
        
        content = await query(fmt, *args, session=session)
        
        if not content['data']['user']['vips']['edges']:
            break
        
        for edge in content['data']['user']['vips']['edges']:
            cursor = edge['cursor']
            yield edge['grantedAt'], edge['node']
        
        if not cursor:
            break

async def query_channel(id=None, name=None, fields=None, *, session):
    assert id or name
    
    fields = fields or ['id', 'name']
    
    fmt = 'query{channel(%s){%s}}'
    arg1 = f'id:{id}' if id else f'name:"{name}"'
    arg2 = ','.join(fields)
    args = (arg1, arg2)
    
    content = await query(fmt, *args, session=session)
    
    return content['data']['channel']

async def query_channels(ids=None, names=None, fields=None, *, session):
    assert ids or names
    
    if ids:
        ids = ','.join(str(id) for id in ids)
    
    if names:
        names = ','.join(f'"{name}"' for name in names)
    
    fields = fields or ['id', 'name']
    
    fmt = 'query{channels(%s){%s}}'
    arg1 = f'ids:[{ids}]' if ids else f'names:[{names}]'
    arg2 = ','.join(fields)
    args = (arg1, arg2)
    
    content = await query(fmt, *args, session=session)
    
    for channel in content['data']['channels']:
        yield channel

async def query_channel_chatters(id=None, name=None, *, session):
    assert id or name
    
    fmt = 'query{channel(%s){chatters{broadcasters{login},moderators{login},staff{login},viewers{login},vips{login}}}}'
    arg = f'id:{id}' if id else f'name:"{name}"'
    
    content = await query(fmt, arg, session=session)
    
    for broadcaster in content['data']['channel']['chatters']['broadcasters']:
        yield 'broadcaster', broadcaster['login']
    
    for moderator in content['data']['channel']['chatters']['moderators']:
        yield 'moderator', moderator['login']
    
    for staff in content['data']['channel']['chatters']['staff']:
        yield 'staff', staff['login']
    
    for viewer in content['data']['channel']['chatters']['viewers']:
        yield 'viewer', viewer['login']
    
    for vip in content['data']['channel']['chatters']['vips']:
        yield 'vip', vip['login']

async def query_game(id=None, name=None, fields=None, *, session):
    assert id or name
    
    fields = fields or ['id', 'name']
    
    fmt = 'query{game(%s){%s}}'
    arg1 = f'id:{id}' if id else f'name:"{name}"'
    arg2 = ','.join(fields)
    args = (arg1, arg2)
    
    content = await query(fmt, *args, session=session)
    
    return content['data']['game']

async def query_game_streams(id=None, name=None, fields=None, *, cursor=None, session):
    assert id or name
    
    fields = fields or ['id']
    cursor = cursor or ''
    
    fmt = 'query{game(%s){streams(after:"%s",first:100,options:{sort:RECENT}){edges{cursor,node{%s}}}}}'
    arg1 = f'id:{id}' if id else f'name:"{name}"'
    arg3 = ','.join(fields)
    
    while True:
        arg2 = cursor
        args = (arg1, arg2, arg3)
        
        content = await query(fmt, *args, session=session)
        
        if not content['data']['game']['streams']['edges']:
            break
        
        for edge in content['data']['game']['streams']['edges']:
            cursor = edge['cursor']
            yield edge['node']
        
        if not cursor:
            break