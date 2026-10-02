"""
OSINT Email Scanner — Custom async engine for deep email intelligence.
Runs in parallel with holehe to extract profile data from 50+ platforms.
"""
import asyncio
import hashlib
import httpx
import json
import re

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
TIMEOUT = 8


# ─── HELPERS ─────────────────────────────────────────────────────────────────
def md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def result(name, domain, exists, rate=False, avatar=None, username=None,
           display_name=None, bio=None, location=None, website=None,
           followers=None, following=None, extra=None):
    return {
        "name": name,
        "domain": domain,
        "exists": exists,
        "rateLimit": rate,
        "avatar": avatar,
        "username": username,
        "display_name": display_name,
        "bio": bio,
        "location": location,
        "website": website,
        "followers": followers,
        "following": following,
        "extra": extra or {},
    }


# ─── PLATFORM CHECKERS ────────────────────────────────────────────────────────

async def check_github(email: str, client: httpx.AsyncClient) -> dict | None:
    """Check GitHub via commit search — returns full profile if found."""
    try:
        headers = {"Accept": "application/vnd.github.v3+json", "User-Agent": UA}
        r = await client.get(
            f"https://api.github.com/search/commits?q=author-email:{email}&per_page=1",
            headers=headers, timeout=TIMEOUT
        )
        if r.status_code != 200:
            return result("GitHub", "github.com", False, rate=(r.status_code == 429))
        items = r.json().get("items", [])
        if not items:
            return result("GitHub", "github.com", False)
        author = items[0].get("author")
        if not author:
            return result("GitHub", "github.com", False)
        uname = author.get("login")
        if not uname:
            return result("GitHub", "github.com", False)
        # Fetch full profile
        r2 = await client.get(f"https://api.github.com/users/{uname}", headers=headers, timeout=TIMEOUT)
        if r2.status_code != 200:
            return result("GitHub", "github.com", True, username=uname,
                          avatar=author.get("avatar_url"))
        u = r2.json()
        return result(
            "GitHub", "github.com", True,
            avatar=u.get("avatar_url"),
            username=u.get("login"),
            display_name=u.get("name"),
            bio=u.get("bio"),
            location=u.get("location"),
            website=u.get("blog"),
            followers=u.get("followers"),
            following=u.get("following"),
            extra={
                "company": u.get("company"),
                "public_repos": u.get("public_repos"),
                "created_at": u.get("created_at"),
                "profile_url": u.get("html_url"),
            }
        )
    except Exception:
        return result("GitHub", "github.com", False, rate=True)


async def check_gravatar(email: str, client: httpx.AsyncClient) -> dict | None:
    try:
        h = md5(email.strip().lower())
        r = await client.get(
            f"https://en.gravatar.com/{h}.json",
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        if r.status_code == 200:
            entry = r.json().get("entry", [])[0]
            return result(
                "Gravatar", "gravatar.com", True,
                avatar=entry.get("thumbnailUrl"),
                username=entry.get("preferredUsername"),
                display_name=entry.get("displayName"),
                bio=entry.get("aboutMe"),
                location=entry.get("currentLocation"),
                website=entry.get("profileUrl"),
            )
        elif r.status_code == 404:
            return result("Gravatar", "gravatar.com", False)
        return result("Gravatar", "gravatar.com", False, rate=True)
    except Exception:
        return result("Gravatar", "gravatar.com", False, rate=True)


async def check_keybase(email: str, client: httpx.AsyncClient) -> dict | None:
    try:
        r = await client.get(
            f"https://keybase.io/_/api/1.0/user/lookup.json?emails={email}",
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        if r.status_code == 200:
            data = r.json()
            them = data.get("them", [])
            if them and them[0]:
                u = them[0]
                basics = u.get("basics", {})
                profile = u.get("profile", {})
                pictures = u.get("pictures", {})
                primary = pictures.get("primary", {})
                return result(
                    "Keybase", "keybase.io", True,
                    avatar=primary.get("url"),
                    username=basics.get("username"),
                    display_name=profile.get("full_name"),
                    bio=profile.get("bio"),
                    location=profile.get("location"),
                    website=profile.get("website"),
                )
            return result("Keybase", "keybase.io", False)
        return result("Keybase", "keybase.io", False, rate=True)
    except Exception:
        return result("Keybase", "keybase.io", False, rate=True)


async def check_duolingo(email: str, client: httpx.AsyncClient) -> dict | None:
    try:
        r = await client.get(
            f"https://www.duolingo.com/2017-06-30/users?email={email}",
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        if r.status_code == 200:
            data = r.json()
            users = data.get("users", [])
            if users:
                u = users[0]
                uname = u.get("username", "")
                if not uname:
                    return result("Duolingo", "duolingo.com", False)
                # Build proper avatar URL
                pic_path = u.get("picture", "")
                if pic_path and not pic_path.startswith("http"):
                    avatar = f"https://simg-ssl.duolingo.com/avatars/{pic_path}/xlarge"
                elif pic_path and pic_path.startswith("http"):
                    avatar = pic_path
                else:
                    avatar = None
                return result(
                    "Duolingo", "duolingo.com", True,
                    avatar=avatar,
                    username=uname,
                    display_name=u.get("name") or uname,
                    extra={
                        "streak": u.get("streak"),
                        "total_xp": u.get("totalXp"),
                        "learning_language": u.get("learningLanguage"),
                        "profile_url": f"https://www.duolingo.com/profile/{uname}",
                    }
                )
            return result("Duolingo", "duolingo.com", False)
        return result("Duolingo", "duolingo.com", False, rate=True)
    except Exception:
        return result("Duolingo", "duolingo.com", False, rate=True)


async def check_chess(email: str, client: httpx.AsyncClient) -> dict | None:
    """Chess.com doesn't have email lookup, but we check via github username if found."""
    return None  # placeholder, filled after github lookup if username known


async def check_reddit_username(username: str, client: httpx.AsyncClient) -> dict | None:
    """If we know the username, check Reddit profile."""
    try:
        r = await client.get(
            f"https://www.reddit.com/user/{username}/about.json",
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        if r.status_code == 200:
            data = r.json().get("data", {})
            icon = data.get("icon_img", "").split("?")[0] if data.get("icon_img") else None
            return result(
                "Reddit", "reddit.com", True,
                avatar=icon or None,
                username=data.get("name"),
                display_name=data.get("subreddit", {}).get("title"),
                extra={
                    "post_karma": data.get("link_karma"),
                    "comment_karma": data.get("comment_karma"),
                    "created": data.get("created_utc"),
                    "profile_url": f"https://reddit.com/u/{data.get('name')}",
                }
            )
        return None
    except Exception:
        return None


async def check_twitter_available(email: str, client: httpx.AsyncClient) -> dict | None:
    try:
        r = await client.get(
            "https://api.twitter.com/i/users/email_available.json",
            params={"email": email},
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        if r.status_code == 200:
            taken = r.json().get("taken", False)
            return result("Twitter / X", "twitter.com", taken)
        return result("Twitter / X", "twitter.com", False, rate=True)
    except Exception:
        return result("Twitter / X", "twitter.com", False, rate=True)


async def check_spotify(email: str, client: httpx.AsyncClient) -> dict | None:
    try:
        r = await client.get(
            "https://spclient.wg.spotify.com/signup/public/v1/account",
            params={"validate": 1, "email": email},
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("status") == 20:
                return result("Spotify", "spotify.com", True)
            return result("Spotify", "spotify.com", False)
        return result("Spotify", "spotify.com", False, rate=True)
    except Exception:
        return result("Spotify", "spotify.com", False, rate=True)

async def check_microsoft(email: str, client: httpx.AsyncClient) -> dict | None:
    try:
        r = await client.post(
            "https://login.microsoftonline.com/common/GetCredentialType?mkt=en-US",
            json={"username": email},
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("IfExistsResult") == 1:
                return result("Microsoft", "microsoft.com", True)
            return result("Microsoft", "microsoft.com", False)
        return result("Microsoft", "microsoft.com", False, rate=True)
    except Exception:
        return result("Microsoft", "microsoft.com", False, rate=True)

async def check_wordpress(email: str, client: httpx.AsyncClient) -> dict | None:
    try:
        r = await client.get(
            f"https://public-api.wordpress.com/rest/v1.1/users/{email}/auth-options",
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        if r.status_code == 200:
            return result("WordPress", "wordpress.com", True)
        elif r.status_code == 404:
            return result("WordPress", "wordpress.com", False)
        return result("WordPress", "wordpress.com", False, rate=True)
    except Exception:
        return result("WordPress", "wordpress.com", False, rate=True)

async def check_unavatar(email: str, client: httpx.AsyncClient) -> dict | None:
    try:
        r = await client.get(
            f"https://unavatar.io/{email}?json=true",
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        if r.status_code == 200:
            url = r.json().get("url", "")
            if url and "favicon.svg" not in url and "fallback.png" not in url:
                return result("Unavatar", "unavatar.io", True, avatar=url)
        return result("Unavatar", "unavatar.io", False)
    except Exception:
        return result("Unavatar", "unavatar.io", False, rate=True)

async def check_platform_forgot_pass(
    name: str, domain: str, client: httpx.AsyncClient,
    method: str, url: str, payload: dict, found_key: str, found_val,
    headers_extra: dict = None, is_post: bool = True
) -> dict | None:
    """Generic forgot-password checker."""
    try:
        hdrs = {"User-Agent": UA, "Content-Type": "application/json"}
        if headers_extra:
            hdrs.update(headers_extra)
        if is_post:
            r = await client.post(url, json=payload, headers=hdrs, timeout=TIMEOUT)
        else:
            r = await client.get(url, params=payload, headers=hdrs, timeout=TIMEOUT)
        if r.status_code in (429, 503):
            return result(name, domain, False, rate=True)
        try:
            data = r.json()
        except Exception:
            data = {}
        exists = False
        if isinstance(found_val, list):
            for fv in found_val:
                if str(fv).lower() in str(data).lower():
                    exists = True
                    break
        elif str(found_val).lower() in str(data).lower():
            exists = True
        return result(name, domain, exists)
    except Exception:
        return result(name, domain, False, rate=True)


# ─── PLATFORMS CONFIG (forgot-password / register checks) ────────────────────
FORGOT_PASS_CHECKS = [
    {
        "name": "Adobe",
        "domain": "adobe.com",
        "url": "https://accounts.adobe.com/api/account/v1/email/confirm",
        "payload_fn": lambda e: {"email": e, "client_id": "HomePage2", "jslVersion": "v2-v0.34.0-4-g37f2e20"},
        "found_val": ["email_exists", "true"],
        "is_post": True,
    },
    {
        "name": "Canva",
        "domain": "canva.com",
        "url": "https://www.canva.com/api/public-auth/v0/user/email-existence",
        "payload_fn": lambda e: {"email": e},
        "found_val": ["REGISTERED"],
        "is_post": True,
    },
    {
        "name": "Dropbox",
        "domain": "dropbox.com",
        "url": "https://api.dropbox.com/1/account/info",
        "payload_fn": lambda e: {"email": e},
        "found_val": ["registered"],
        "is_post": True,
    },
    {
        "name": "Quizlet",
        "domain": "quizlet.com",
        "url": "https://quizlet.com/webapi/3.4/user-password-reset-request",
        "payload_fn": lambda e: {"usernameOrEmail": e},
        "found_val": ["email_sent", "success", "true"],
        "is_post": True,
    },
    {
        "name": "Plex",
        "domain": "plex.tv",
        "url": "https://plex.tv/api/v2/users/reset-password-link",
        "payload_fn": lambda e: {"email": e},
        "found_val": ["success", "sent"],
        "is_post": True,
    },
    {
        "name": "Zoho",
        "domain": "zoho.com",
        "url": "https://accounts.zoho.com/api/v1/auth/verify/email",
        "payload_fn": lambda e: {"email": e},
        "found_val": ["EXIST", "exist"],
        "is_post": True,
    },
    {
        "name": "Wix",
        "domain": "wix.com",
        "url": "https://users.wix.com/wix-sm/api/v1/auth/forgot-password",
        "payload_fn": lambda e: {"email": e},
        "found_val": ["success", "EMAIL_SENT"],
        "is_post": True,
    },
    {
        "name": "Snapchat",
        "domain": "snapchat.com",
        "url": "https://accounts.snapchat.com/accounts/send_password_reset_email",
        "payload_fn": lambda e: {"email": e},
        "found_val": ["success", "sent"],
        "is_post": True,
    },
    {
        "name": "Discord",
        "domain": "discord.com",
        "url": "https://discord.com/api/v9/auth/forgot",
        "payload_fn": lambda e: {"login": e},
        "found_val": ["success", "{}"],
        "is_post": True,
    },
    {
        "name": "Pinterest",
        "domain": "pinterest.com",
        "url": "https://api.pinterest.com/v3/pidgets/users/",
        "payload_fn": lambda e: {"email": e},
        "found_val": ["data", "username"],
        "is_post": False,
    },
    {
        "name": "Coursera",
        "domain": "coursera.org",
        "url": "https://api.coursera.org/api/login/v3",
        "payload_fn": lambda e: {"email": e, "verifyEmailType": "register"},
        "found_val": ["exists"],
        "is_post": True,
    },
    {
        "name": "Fiverr",
        "domain": "fiverr.com",
        "url": "https://www.fiverr.com/validate_username",
        "payload_fn": lambda e: {"email": e},
        "found_val": ["exists"],
        "is_post": True,
    },
    {
        "name": "Ebay",
        "domain": "ebay.com",
        "url": "https://signin.ebay.com/ws/eBayISAPI.dll?SignInFromFPP&email=",
        "payload_fn": lambda e: {"userid": e},
        "found_val": ["password", "account"],
        "is_post": False,
    },
    {
        "name": "Patreon",
        "domain": "patreon.com",
        "url": "https://www.patreon.com/api/auth?include=user&fields[user]=email",
        "payload_fn": lambda e: {"email": e},
        "found_val": ["data"],
        "is_post": False,
    },
    {
        "name": "Codecademy",
        "domain": "codecademy.com",
        "url": "https://www.codecademy.com/users",
        "payload_fn": lambda e: {"email": e},
        "found_val": ["exists"],
        "is_post": False,
    },
    {
        "name": "Replit",
        "domain": "replit.com",
        "url": "https://replit.com/api/v0/users/",
        "payload_fn": lambda e: {"email": e},
        "found_val": ["exists", "id"],
        "is_post": False,
    },
]


# ─── MAIN SCANNER ─────────────────────────────────────────────────────────────
async def scan_email(email: str) -> dict:
    email_lower = email.strip().lower()
    
    profiles = []
    platform_results = []

    limits = httpx.Limits(max_connections=30, max_keepalive_connections=15)
    async with httpx.AsyncClient(verify=False, limits=limits, follow_redirects=True) as client:
        
        # Core enrichment (returns full profiles or exact checks)
        enrichment_tasks = [
            check_github(email_lower, client),
            check_gravatar(email_lower, client),
            check_keybase(email_lower, client),
            check_duolingo(email_lower, client),
            check_unavatar(email_lower, client),
            check_spotify(email_lower, client),
            check_microsoft(email_lower, client),
            check_wordpress(email_lower, client),
            check_twitter_available(email_lower, client),
        ]
        enrichment_results = await asyncio.gather(*enrichment_tasks, return_exceptions=True)
        
        for r in enrichment_results:
            if r and not isinstance(r, Exception):
                if r["exists"]:
                    profiles.append(r)
                else:
                    platform_results.append(r)

        # Forgot-password platform checks (bulk)
        bulk_tasks = []
        for cfg in FORGOT_PASS_CHECKS:
            bulk_tasks.append(
                check_platform_forgot_pass(
                    name=cfg["name"],
                    domain=cfg["domain"],
                    client=client,
                    method="forgot-password",
                    url=cfg["url"],
                    payload=cfg["payload_fn"](email_lower),
                    found_key="",
                    found_val=cfg["found_val"],
                    is_post=cfg.get("is_post", True),
                )
            )
        bulk_results = await asyncio.gather(*bulk_tasks, return_exceptions=True)
        for r in bulk_results:
            if r and not isinstance(r, Exception):
                platform_results.append(r)

    # Collect all unique names, usernames, avatars, locations from profiles
    all_avatars = [p["avatar"] for p in profiles if p.get("avatar")]
    all_names = list({p["display_name"] for p in profiles if p.get("display_name")})
    all_usernames = list({p["username"] for p in profiles if p.get("username")})
    all_locations = list({p["location"] for p in profiles if p.get("location")})

    confirmed = [p["domain"] for p in profiles if p["exists"]]
    confirmed += [p["domain"] for p in platform_results if p["exists"]]
    
    rate_limited = [p["domain"] for p in platform_results if p["rateLimit"] and not p["exists"]]

    return {
        "success": True,
        "email": email,
        "profiles": [p for p in profiles if p["exists"]],
        "found": confirmed,
        "rate_limited": rate_limited,
        "names": all_names,
        "usernames": all_usernames,
        "avatars": all_avatars,
        "locations": all_locations,
        "total_checked": len(profiles) + len(platform_results),
        "count": len(confirmed),
    }
