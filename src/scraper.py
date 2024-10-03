import modal
import uuid
import json
init_blob = '''You are a data extraction expert.

I will give you a an html blob and a list of fields to extract.

Extract the fields in JSON format.

If there is relevant data and the fields are:
price, rating
return:
{
    "price": ["$12.33"],
    "rating": [9]
}

If there is no relevant data return
{
    field1: []
}

example 1:
<tr><td><a href='\"/title/tt0468569/?pf_rd_m=A2FGELUUNOQJNL&amp;pf_rd_p=1a264172-ae11-42e4-8ef7-7fed1973bb8f&amp;pf_rd_r=WC4KT4219389D45E99HP&amp;pf_rd_s=center-1&amp;pf_rd_t=15506&amp;pf_rd_i=top&amp;ref_=chttp_tt_3\"'></a></td><td> 3. <a (dir.),="" bale,="" christian="" heath="" href='\"/title/tt0468569/?pf_rd_m=A2FGELUUNOQJNL&amp;pf_rd_p=1a264172-ae11-42e4-8ef7-7fed1973bb8f&amp;pf_rd_r=WC4KT4219389D45E99HP&amp;pf_rd_s=center-1&amp;pf_rd_t=15506&amp;pf_rd_i=top&amp;ref_=chttp_tt_3\"' ledger\"="" nolan="" title='\"Christopher'>The Dark Knight</a><span>(2008)</span></td><td imdbrating\"=""><strong 2,682,223="" based="" on="" ratings\"="" title='\"9.0' user="">9.0</strong></td><td><div seen-widget-tt0468569\"=""><div><div><span> </span><ol><li>1</li><li>2</li><li>3</li><li>4</li><li>5</li><li>6</li><li>7</li><li>8</li><li>9</li><li>10</li></ol></div></div><div><div>NOT YET RELEASED</div><div> </div><div>Seen</div></div></div></td></tr>

title, rating
{
    "title": ["The Dark Knight"],
    "rating": [9.0]
}

example 2:
<head><div id="monetate_selectorHTML_851509f7_0">&lt;style&gt;&lt;/style&gt;</div><title> Sale | Specialized.com</title></head>
<span aria-pressed="false" role="button"> Use Website In a Screen-Reader Mode </span>
<div aria-label="Skip Links" role="region"> <a href="#acsbContent"> Skip to Content <div aria-hidden="true"><span>↵</span>ENTER </div> </a> <a href="#acsbMenu"> Skip to Menu <div aria-hidden="true"><span>↵</span>ENTER</div> </a> <a href="#acsbFooter"> Skip to Footer <div aria-hidden="true"><span>↵</span>ENTER</div> </a></div>
<div aria-label="Open accessiBe: accessibility options, statement and help" role="button"> <span> </span></div>
<a href="#content">Skip to main content</a>
<div id="header-top"><div><div><div><p><a href="https://www.specialized.com/faq#what-are-the-shipping-options-for-specialized.com?" rel="nofollow">Free shipping on orders $50+, except bikes.</a></p></div></div><div><div><a href="https://www.specialized.com/us/en/store-finder" target="_self">Find a Retailer</a></div><div>EN<div><a href="https://www.specialized.com/us/en" target="_self">US: English</a><a href="https://www.specialized.com/us/en/location" target="_self">Change region</a></div></div><div><a href="https://www.specialized.com/us/en/account/login" target="_self">Sign in</a></div></div></div></div>

name, price
{
    "name": [],
    "price": []
}

If there are missing fields.
Make sure the pad the missing fields, all of the lists should be the same size.

ex.
<td><div class="center"><div class="floatnone"><a href="/File:Warrior_Helmet.png" class="image"><img alt="Warrior Helmet.png" src="/mediawiki/images/6/68/Warrior_Helmet.png" width="54" height="54"></a></div></div>
</td>
<td><a href="/Warrior_Helmet" title="Warrior Helmet">Warrior Helmet</a>
</td>
<td>An Ostrich eggshell repurposed into a helmet.
</td>
<td>
</td>
<td><a href="/Tailoring" title="Tailoring">Tailoring</a>
</td></tr>
<tr>
<td><div class="center"><div class="floatnone"><a href="/File:ConcernedApe_Hat.png" class="image"><img alt="ConcernedApe Hat.png" src="/mediawiki/images/1/1a/ConcernedApe_Hat.png" width="57" height="57"></a></div></div>
</td>
<td><a href="/%3F%3F%3F_(hat)" title="??? (hat)">???</a>
</td>
<td>???
</td>
<td>
</td>
<td>Interact with the monkey in the <a href="/Volcano_Caldera" title="Volcano Caldera" class="mw-redirect">Volcano Caldera</a> once you have achieved 100% <a href="/Perfection" title="Perfection">Perfection</a>.
</td></tr>

image, name, description, achievement, obtain
{
    "image": ["/File:Warrior_Helmet.png", "/File:ConcernedApe_Hat.png"]
    "name": ["Warrior Helmet", "???"],
    "description": ["An Ostrich eggshell repurposed into a helmet.", "???"],
    "achievement": ["MISSING", "MISSING"],
    "obtain": ["Tailoring", "Interact with the monkey in the Volcano Caldera once you have achieved 100% Perfection."]
}

ONLY OUTPUT JSON. Make sure your response is in valid JSON. DO NOT INCLUDE ANY EXTRA TEXT BEYOND WHAT IS REQUIRED.
'''

request_blob = '''
{content}

{fields}
'''

app = modal.App("gpt-webscraper-jobs")


def fetchJob(uid, sid):
    import firebase_admin
    from firebase_admin import credentials, firestore
    import json

    # Load the service account key JSON file
    service_account_path = '/root/gpt-scraper-SA.json'

    # Initialize the Firebase app with the service account
    if not firebase_admin._apps:
        cred = credentials.Certificate(service_account_path)
        firebase_admin.initialize_app(cred)

    # Get a reference to the Firestore database
    db = firestore.client()

    # Construct the full path to the job document
    job_path = f"User/{uid}/Scrape/{sid}"

    # Fetch the job document
    job_ref = db.document(job_path)
    job = job_ref.get()

    if job.exists:
        job_data = job.to_dict()
        job_data['id'] = job_path  # Add the document path as 'id'
        return job_data
    else:
        return None  # Return None if the job doesn't exist


def updateJob(job, fields_to_update):
    import firebase_admin
    from firebase_admin import credentials, firestore
    import json

    service_account_path = '/root/gpt-scraper-SA.json'

    # Initialize the Firebase app with the service account
    if not firebase_admin._apps:
        cred = credentials.Certificate(service_account_path)
        firebase_admin.initialize_app(cred)

    db = firestore.client()
    job_ref = db.document(job["id"])
    if 'done' in fields_to_update:
        fields_to_update['done'] = firestore.firestore.SERVER_TIMESTAMP
    job_ref.update(fields_to_update)


def query_anthropic(system_prompt, user_message):
    import anthropic
    import os
    print("calling anthro")
    client = anthropic.Anthropic(
        api_key=os.environ["ANTHROPIC_API_TOKEN"],
    )
    message = client.messages.create(
        model="claude-3-5-sonnet-20240620",
        max_tokens=1000,
        temperature=0,
        system=system_prompt,
        messages=[
            {
                "role": "user", 
                "content": [
                    {
                        "type": "text",
                        "text": user_message
                    }
                ]
            }
        ]
    )
    return message.content[0].text

def parse(fields, chunk):
    csv_injected = request_blob.format(content=chunk, fields=fields)
    resp = query_anthropic(init_blob, csv_injected)
    return resp

playwright_image = modal.Image.debian_slim(python_version="3.10").run_commands(
    "apt-get update",
    "apt-get install -y software-properties-common",
    "apt-add-repository non-free",
    "apt-add-repository contrib",
    "apt-get update",
    "pip install playwright==1.30.0",
    "playwright install-deps chromium",
    "playwright install chromium",
    "pip install beautifulsoup4==4.12.2",
    "pip install firebase-admin",
    "pip install azure-storage-blob==12.16.0",
    "pip install transformers==4.30.2",
    "pip install anthropic",
).copy_local_file("gpt-scraper-SA.json", "/root/gpt-scraper-SA.json")

openai_image = modal.Image.debian_slim(python_version="3.11").run_commands(
    "apt-get update",
    "apt-get install -y software-properties-common",
    "apt-add-repository non-free",
    "apt-add-repository contrib",
    "apt-get update",
    "pip install anthropic",
    "pip install beautifulsoup4==4.12.2",
)

@app.function()
def square(x):
    print("This code is running on a remote worker!")
    return x**2

def deepCleanSoup(soup):
    from bs4 import Tag, Comment
    blocked_tags = set([
        "br",
        "canvas",
        "circle",
        "defs",
        "desc",
        "g",
        "iframe",
        "img",
        "lineargradient",
        "link",
        "meta",
        "noscript",
        "path",
        "script",
        "stop",
        "style",
        "svg",
        "symbol",
    ])
    cleaning_regex = r'[\n\s]+'
    keep_tags = set([
        "a"
    ])
    for tag in blocked_tags:
        for s in soup.select(tag):
            s.extract()
    for tag in soup.find_all(lambda t: any(i.startswith('data-') for i in t.attrs)):
        for attr in list(tag.attrs):
            if attr.startswith('data-'):
                del tag.attrs[attr]
    comments = soup.findAll(text=lambda text: isinstance(text, Comment))
    [comment.extract() for comment in comments]
    q = [soup]
    while q:
        cur = q.pop()
        if isinstance(cur, Tag):
            for child in cur.contents:
                q.append(child)
        else:
            if cur == "\n":
                cur.extract()
                continue
    in_rem = True
    while in_rem:
        extracted = False
        for tag in soup.find_all():
            if not tag.contents and tag.name not in keep_tags:
                tag.decompose()
                extracted = True
        in_rem = extracted
    for tag in soup.find_all():
        if 'data-bind' in tag.attrs:
            del tag["data-bind"]
        if 'style' in tag.attrs:
            del tag["style"]
        if 'class' in tag.attrs:
            del tag["class"]
        if 'tabindex' in tag.attrs:
            del tag["tabindex"]
        if 'click' in tag.attrs:
            del tag["click"]
    return soup


def concatChunks(sentences, max_len):
    from transformers import GPT2TokenizerFast
    tokenizer = GPT2TokenizerFast.from_pretrained("gpt2")
    if len(sentences) == 0:
        return []
    results = []
    top = sentences.pop(0)
    while len(sentences) > 0:
        temp = "\n".join([top, sentences[0]])
        if len(tokenizer.tokenize(temp)) > max_len:
            results.append(top)
            top = sentences.pop(0)
        else:
            sentences.pop(0)
            top = temp
    if len(top) > 0:
        results.append(top)
    return results


def dfsGenChunks(soup, limit):
    from transformers import GPT2TokenizerFast
    from bs4 import Tag, Comment
    import re
    tokenizer = GPT2TokenizerFast.from_pretrained("gpt2")
    cleaning_regex = r'[\n\s]+'
    chunks = []
    stack = [soup]
    while stack:
        cur = stack.pop()
        tokenizedAmount = len(tokenizer.tokenize(str(cur)))
        if tokenizedAmount < limit:
            # need to append tokens here too
            chunks.append(re.sub(cleaning_regex, ' ', str(cur)))
            continue
        else:
            if isinstance(cur, Tag):
                for child in cur.contents[::-1]:
                    stack.append(child)
    return chunks


def genChunks(soup, limit):
    cleaned = deepCleanSoup(soup)
    decomposedChunks = dfsGenChunks(cleaned, limit)
    chunks = concatChunks(decomposedChunks, limit)
    return chunks


def writeToBlobStore(container, contents):
    import os
    from azure.storage.blob import BlobClient
    blob_id = str(uuid.uuid4())
    client = BlobClient.from_connection_string(
        conn_str=os.environ['AZURE_BLOB_CONNECTION_STRING'],
        container_name=container, blob_name=blob_id)
    if isinstance(contents, dict):
        contents = json.dumps(contents)
    client.upload_blob(contents)
    return blob_id

@app.function(image=openai_image,
               timeout=600,
               concurrency_limit=2,
               retries=modal.Retries(
                   max_retries=3,
                   backoff_coefficient=2.0,
                   initial_delay=20.0,
               ),
               secrets=[
                modal.Secret.from_name("my-custom-secret"),
            ])
def parseBlob(fields: str, chunk: str):
    print("in parse blob")
    res = parse(fields, chunk)
    return res

async def processJob(job):
    print("in process job")
    import os
    import traceback
    from playwright._impl._api_types import TimeoutError as PlaywrightTimeoutError

    auth = os.environ['BRIGHT_DATA_AUTH_TOKEN']
    browser_url = f'wss://{auth}@brd.superproxy.io:9222'
    fakeErr = False
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(browser_url, timeout=60000)
            context = await browser.new_context()
            
            page = await context.new_page()
            page.on("response", lambda response: print(
                f"Response received from {response.url} with status {response.status}"))
            
            try:
                await page.goto(job["url"], wait_until="domcontentloaded", timeout=120000)
            except PlaywrightTimeoutError:
                print("Page load timed out. Continuing with partial content...")
                # You might want to add some error handling or retry logic here
            
            await page.wait_for_timeout(5000)
            # scroll to bottom once
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2000)
            html = await page.evaluate("() => document.documentElement.outerHTML")
            job["html"] = html
            print("set html")
            await context.close()
            await browser.close()
            fakeErr = True
        return job
    except Exception as exc:
        print(f"Fake error: {fakeErr}")
        if fakeErr:
            return job
        print(f"Error in processJob: {exc}")
        print(traceback.format_exc())
        updateJob(job, {"status": "error", "error_message": str(exc)})
        job["status"] = "error"
        job["error_message"] = str(exc)
        return job

# @stub.local_entrypoint()
@app.function(image=playwright_image,
              secrets=[
                modal.Secret.from_name("my-custom-secret"),
            ])
async def run(uid, sid):
    from bs4 import BeautifulSoup 
    print(f"in run")
    job = fetchJob(uid, sid)  # Assume this function exists to fetch a single job by ID
    if not job:
        print(f"Job with ID {sid} not found")
        return
    print(f"updating job to running")
    updateJob(job, {"status": "running"})
    
    try:
        print(f"processing")
        processed_job = await processJob(job)
        print("job done prcoessing")
        
        if "status" in processed_job and processed_job["status"] == "error":
            print(processed_job)
            return
        print("writing to blob store")
        # Write HTML to Azure bucket and update job
        blobId = writeToBlobStore("html", processed_job["html"])
        print("wrote to blob store")
        updateJob(processed_job, {"html_blob_id": blobId})
        print("updating job html_blob_id")
        print("processing soup")
        soup = BeautifulSoup(processed_job["html"], 'html.parser')
        portions = genChunks(soup, 20_000)
        print("generated chunks")
        readyToParse = [(processed_job["fields"], p) for p in portions]
        cleaned = []
        print(readyToParse)
        # results = parseBlob.map(readyToParse)
        async for res in parseBlob.starmap.aio(readyToParse):
            print(res)
            if len(res) > 0:
                cleaned.append(res)
        print("parsed")

        result = {}
        try:
            for string in cleaned:
                dict_obj = json.loads(string)
                for key, value in dict_obj.items():
                    if key in result:
                        result[key] += value
                    else:
                        result[key] = value
        except Exception as exc:
            print(exc)
            result = "\n".join(cleaned)

        # Write result to Azure bucket and update job
        resultId = writeToBlobStore("scrape", result)
        updateJob(processed_job, {"result_id": resultId, "status": "done", "done": True})
        
    except Exception as exc:
        print(exc)
        updateJob(job, {"status": "error"})
        raise exc

