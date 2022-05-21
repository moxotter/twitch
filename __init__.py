from datetime import datetime

import aiohttp

import twitch.gql
import twitch.oauth

class User:
    def __init__(self, id, login):
        self.id = id
        self.login = login
    
    def __repr__(self):
        return f'User(id={repr(self.id)}, login={repr(self.login)})'

class PartialUser:
    def __init__(self, id=None, login=None):
        self.id = id
        self.login = login
    
    def __repr__(self):
        return f'PartialUser(id={repr(self.id)}, login={repr(self.login)})'

class Channel:
    def __init__(self, id, name):
        self.id = id
        self.name = name
    
    def __repr__(self):
        return f'Channel(id={repr(self.id)}, name={repr(self.name)})'

class Client:
    def __init__(self):
        self.session = aiohttp.ClientSession()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        await self.close()
    
    async def close(self):
        await self.session.close()
    
    async def fetch_user(self, id=None, login=None):
        assert id or login
        user_data = await twitch.gql.query_user(id, login, session=self.session)
        if not user_data:
            return None
        user = User(int(user_data['id']), user_data['login'])
        return user
    
    async def fetch_users(self, ids=None, logins=None):
        assert ids or logins
        async for user_data in twitch.gql.query_users(ids, logins, session=self.session):
            if not user_data:
                yield None
            user = User(int(user_data['id']), user_data['login'])
            yield user
    
    async def fetch_user_followers(self, id=None, login=None, after=None):
        assert id or login
        cursor = twitch.gql.build_cursor_followers(id, after) if after else None
        async for followed_at, user_data in twitch.gql.query_user_followers(id=id, login=login, cursor=cursor, session=self.session):
            followed_at = datetime.fromisoformat(followed_at[:-1])
            user = User(int(user_data['id']), user_data['login'])
            yield followed_at, user
    
    async def fetch_user_follows(self, id=None, login=None, after=None):
        assert id or login
        cursor = twitch.gql.build_cursor_follows(id, after) if after else None
        async for followed_at, user_data in twitch.gql.query_user_follows(id=id, login=login, cursor=cursor, session=self.session):
            followed_at = datetime.fromisoformat(followed_at[:-1])
            user = User(int(user_data['id']), user_data['login'])
            yield followed_at, user
    
    async def fetch_user_mods(self, id=None, login=None, after=None):
        assert id or login
        cursor = twitch.gql.build_cursor_mods(id, after) if after else None
        async for granted_at, user_data in twitch.gql.query_user_mods(id=id, login=login, cursor=cursor, session=self.session):
            granted_at = datetime.fromisoformat(granted_at.split('.')[0])
            user = User(int(user_data['id']), user_data['login'])
            yield granted_at, user
    
    async def fetch_user_vips(self, id=None, login=None, after=None):
        assert id or login
        cursor = twitch.gql.build_cursor_vips(id, after) if after else None
        async for granted_at, user_data in twitch.gql.query_user_vips(id=id, login=login, cursor=cursor, session=self.session):
            granted_at = datetime.fromisoformat(granted_at.split('.')[0])
            user = User(int(user_data['id']), user_data['login'])
            yield granted_at, user
    
    async def fetch_channel(self, id=None, name=None):
        assert id or name
        channel_data = await twitch.gql.query_channel(id, name, session=self.session)
        if not channel_data:
            return None
        channel = Channel(int(channel_data['id']), channel_data['name'])
        return channel
    
    async def fetch_channels(self, ids=None, names=None):
        assert ids or names
        async for channel_data in twitch.gql.query_channels(ids, names, session=self.session):
            if not channel_data:
                yield None
            channel = Channel(int(channel_data['id']), channel_data['name'])
            yield channel
    
    async def fetch_channel_chatters(self, id=None, name=None):
        assert id or name
        async for category, login in twitch.gql.query_channel_chatters(id, name, session=self.session):
            user = PartialUser(login=login)
            yield category, user