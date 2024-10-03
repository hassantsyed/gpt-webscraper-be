import fastapi
import modal

app = modal.App("gpt-webscraper-webapp")
web_app = fastapi.FastAPI()

@web_app.post("/parse")
async def parse(request: fastapi.Request):
    try:
        body = await request.json()
        uid = body.get('uid')
        sid = body.get('sid')
        fn = modal.Function.lookup("gpt-webscraper-jobs", "run")
        call = fn.spawn(uid, sid)
        return {"result": call.object_id}
    except Exception as e:
        return {"error": str(e)}

@app.function()
@modal.asgi_app()
def wrapper():
    return web_app