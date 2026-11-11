"""NEXUS Remote Browser: one isolated Chromium context per WebSocket connection.
Intended as a small private/demo service, not an open commercial browsing proxy.
"""
import asyncio
import ipaddress
import json
import os
import re
import socket
import time
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from urllib.parse import quote, urlsplit

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from playwright.async_api import async_playwright, Error as PlaywrightError

ROOT = Path(__file__).resolve().parent
MAX_SESSIONS = int(os.getenv('MAX_SESSIONS', '2'))
IDLE_SECONDS = int(os.getenv('IDLE_SECONDS', '900'))
VIEWPORT_W = 1150
VIEWPORT_H = 740
BROWSER = None
PW = None
active_sessions = 0
session_lock = asyncio.Lock()

BLOCKED_HOST_SUFFIXES = ('.local', '.localhost', '.internal', '.test', '.home', '.lan', '.onion')

async def public_web_address(url: str):
    if not isinstance(url, str) or len(url) > 2048:
        return False
    try:
        parsed = urlsplit(url)
        if parsed.scheme not in ('http','https') or not parsed.hostname or parsed.username or parsed.password:
            return False
        if parsed.port not in (None, 80, 443):
            return False
        host = parsed.hostname.lower().rstrip('.')
        if host == 'localhost' or host.endswith(BLOCKED_HOST_SUFFIXES) or '.' not in host:
            return False
        try:
            ip = ipaddress.ip_address(host)
            return ip.is_global
        except ValueError:
            pass
        results = await asyncio.wait_for(asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM), 3)
        return bool(results) and all(ipaddress.ip_address(r[4][0]).is_global for r in results)
    except (ValueError, OSError, asyncio.TimeoutError):
        return False

async def route_guard(route):
    url = route.request.url
    # Chromium internal pages / assets are not fetched from user-provided networks.
    if url.startswith(('data:', 'blob:', 'about:')):
        await route.continue_()
        return
    if await public_web_address(url):
        await route.continue_()
    else:
        await route.abort('blockedbyclient')

@asynccontextmanager
async def lifespan(app):
    global PW, BROWSER
    PW = await async_playwright().start()
    BROWSER = await PW.chromium.launch(headless=True, executable_path=os.getenv('NEXUS_CHROMIUM_PATH') or None, args=['--disable-dev-shm-usage','--no-first-run'])
    try:
        yield
    finally:
        await BROWSER.close()
        await PW.stop()

app = FastAPI(lifespan=lifespan)

@app.get('/')
async def index():
    return FileResponse(ROOT / 'index.html', headers={'Cache-Control':'no-store','X-Content-Type-Options':'nosniff'})

@app.get('/health')
async def health():
    return JSONResponse({'status':'ok','engine':'chromium','sessions':active_sessions,'capacity':MAX_SESSIONS})

@app.websocket('/ws')
async def browser_socket(ws: WebSocket):
    global active_sessions
    # Prevent other websites from attaching browsers on a user's behalf.
    origin = ws.headers.get('origin','')
    host = ws.headers.get('host','')
    if origin not in (f'https://{host}', f'http://{host}', 'null'):
        await ws.close(code=1008, reason='Origin blocked')
        return
    async with session_lock:
        if active_sessions >= MAX_SESSIONS:
            await ws.accept()
            await ws.send_json({'type':'error','message':'NEXUS is at capacity. Try again shortly.'})
            await ws.close(code=1013)
            return
        active_sessions += 1
    context = None
    send_lock = asyncio.Lock()
    pages = []
    active = 0
    last_seen = time.monotonic()
    changed = asyncio.Event()
    async def send(data):
        async with send_lock:
            await ws.send_json(data)
    async def state():
        tabs = [{'id':i,'title':(await p.title())[:65] or 'New tab','url':p.url} for i,p in enumerate(pages)]
        await send({'type':'state','tabs':tabs,'active':active})
    async def fresh_tab(url='about:blank'):
        nonlocal active
        p = await context.new_page()
        await p.route('**/*', route_guard)
        p.on('framenavigated', lambda frame: changed.set())
        pages.append(p)
        active = len(pages)-1
        if url != 'about:blank':
            await goto(url)
        changed.set()
    async def goto(value):
        nonlocal last_seen
        val = str(value).strip()[:2048]
        if not val:
            return
        if re.match(r'^https?://',val,re.I):
            dest = val
        elif re.match(r'^(?:[a-z0-9-]+\.)+[a-z]{2,}(?::\d+)?(?:[/?#].*)?$',val,re.I):
            dest = 'https://' + val
        else:
            dest = 'https://www.google.com/search?q=' + quote(val)
        if not await public_web_address(dest):
            await send({'type':'error','message':'This address is unavailable or blocked for server safety.'})
            return
        try:
            await pages[active].goto(dest,wait_until='domcontentloaded',timeout=22000)
        except PlaywrightError as exc:
            await send({'type':'notice','message':'Page loading issue: '+str(exc)[:160]})
        changed.set()
    async def frames():
        while True:
            await asyncio.sleep(0.85)
            if time.monotonic()-last_seen > IDLE_SECONDS:
                await send({'type':'error','message':'Session ended after inactivity. Refresh to reconnect.'})
                await ws.close(code=1000)
                return
            if not pages or active >= len(pages):
                continue
            p = pages[active]
            try:
                frame = await p.screenshot(type='jpeg',quality=58, timeout=7000, animations='disabled')
                async with send_lock:
                    await ws.send_bytes(frame)
                if changed.is_set():
                    changed.clear()
                    await state()
            except (PlaywrightError, WebSocketDisconnect, RuntimeError):
                continue
    await ws.accept()
    frame_task = None
    try:
        context = await BROWSER.new_context(viewport={'width':VIEWPORT_W,'height':VIEWPORT_H},device_scale_factor=1,accept_downloads=False,permissions=[],service_workers='block',java_script_enabled=True)
        await context.route('**/*',route_guard)
        await fresh_tab()
        await send({'type':'ready','width':VIEWPORT_W,'height':VIEWPORT_H})
        await state()
        frame_task = asyncio.create_task(frames())
        while True:
            message = await ws.receive_text()
            last_seen = time.monotonic()
            try:
                msg = json.loads(message)
                action = msg.get('action','')
                page = pages[active]
                if action == 'navigate':
                    await goto(msg.get('value',''))
                elif action == 'back':
                    with suppress(PlaywrightError):
                        await page.go_back(wait_until='domcontentloaded',timeout=13000)
                elif action == 'forward':
                    with suppress(PlaywrightError):
                        await page.go_forward(wait_until='domcontentloaded',timeout=13000)
                elif action == 'reload':
                    with suppress(PlaywrightError):
                        await page.reload(wait_until='domcontentloaded',timeout=16000)
                elif action == 'home':
                    await goto('https://www.google.com/')
                elif action == 'new_tab' and len(pages) < 5:
                    await fresh_tab()
                elif action == 'switch':
                    i = int(msg.get('id',0))
                    if 0 <= i < len(pages):
                        active = i
                elif action == 'close_tab':
                    i = int(msg.get('id',-1))
                    if 0 <= i < len(pages) and len(pages)>1:
                        await pages.pop(i).close()
                        active = min(max(0, active-(i<active)),len(pages)-1)
                elif action == 'click':
                    x = max(0,min(VIEWPORT_W-1,float(msg.get('x',0))))
                    y = max(0,min(VIEWPORT_H-1,float(msg.get('y',0))))
                    button = msg.get('button','left')
                    await page.mouse.click(x,y,button=button if button in ('left','right','middle') else 'left')
                elif action == 'move':
                    await page.mouse.move(float(msg.get('x',0)),float(msg.get('y',0)))
                elif action == 'scroll':
                    await page.mouse.wheel(max(-1200,min(1200,float(msg.get('dx',0)))),max(-1200,min(1200,float(msg.get('dy',0)))))
                elif action == 'key':
                    key = str(msg.get('key',''))[:40]
                    if key and re.fullmatch(r'[a-zA-Z0-9+\-]+|Arrow(Up|Down|Left|Right)|Backspace|Delete|Enter|Tab|Escape|Space|Home|End|PageUp|PageDown',key):
                        await page.keyboard.press(key)
                elif action == 'text':
                    await page.keyboard.insert_text(str(msg.get('value',''))[:800])
                elif action == 'ping':
                    await send({'type':'pong'})
                changed.set()
            except (ValueError, TypeError, PlaywrightError) as exc:
                await send({'type':'notice','message':str(exc)[:160]})
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        with suppress(Exception):
            await send({'type':'error','message':'Browser session ended: '+str(exc)[:140]})
    finally:
        if frame_task:
            frame_task.cancel()
            with suppress(asyncio.CancelledError):
                await frame_task
        if context:
            with suppress(Exception):
                await context.close()
        async with session_lock:
            active_sessions -= 1
