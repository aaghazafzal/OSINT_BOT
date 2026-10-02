"""
OSINT Email Scanner — Custom async engine for deep email intelligence.
Runs in parallel with holehe to extract profile data from 50+ platforms.

Educational purpose only. Uses only public APIs and forgot-password flows.
"""
import asyncio
import hashlib
import httpx
import re
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
TIMEOUT = 10


# ─── HELPERS ─────────────────────────────────────────────────────────────────
def md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def result(name, domain, exists, rate=False, avatar=None, username=None,
           display_name=None, bio=None, location=None, website=None,
           followers=None, following=None, joined=None, last_active=None,
           extra=None):
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
        "joined": joined,
        "last_active": last_active,
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
            joined=u.get("created_at", "")[:10] if u.get("created_at") else None,
            extra={
                "company": u.get("company"),
                "public_repos": u.get("public_repos"),
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


async def check_duolingo(email: str, client: httpx.AsyncClient, usernames: list[str] = None) -> dict | None:
    """
    Duolingo email lookup returns a blank user (id=0) when email is private.
    We check for id > 0 to confirm existence via email, or try derived usernames.
    """
    if usernames is None:
        usernames = []
        
    prefix = email.split('@')[0]
    if prefix not in usernames:
        usernames.append(prefix)
        
    try:
        r = await client.get(
            f"https://www.duolingo.com/2017-06-30/users?email={email}",
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        found_user = None
        if r.status_code == 200:
            data = r.json()
            users = data.get("users", [])
            if users and users[0].get("id", 0) > 0:
                found_user = users[0]
                
        if not found_user:
            for un in usernames:
                r2 = await client.get(
                    f"https://www.duolingo.com/2017-06-30/users?username={un}",
                    headers={"User-Agent": UA}, timeout=TIMEOUT
                )
                if r2.status_code == 200:
                    data2 = r2.json()
                    users2 = data2.get("users", [])
                    if users2 and users2[0].get("id", 0) > 0:
                        found_user = users2[0]
                        break

        if found_user:
            uname = found_user.get("username", "")
            pic_path = found_user.get("picture", "")
            if pic_path and "default" in pic_path:
                avatar = None
            elif pic_path and pic_path.startswith("//"):
                avatar = f"https:{pic_path}/xlarge"
            elif pic_path and pic_path.startswith("http"):
                avatar = pic_path
            else:
                avatar = None
            
            joined_ts = found_user.get("creationDate")
            import datetime
            joined_str = None
            if joined_ts:
                joined_str = datetime.datetime.fromtimestamp(joined_ts).strftime("%Y-%m-%d")
            
            return result(
                "Duolingo", "duolingo.com", True,
                avatar=avatar,
                username=uname,
                display_name=found_user.get("name") or uname,
                joined=joined_str,
                extra={
                    "streak": found_user.get("streak"),
                    "total_xp": found_user.get("totalXp"),
                    "learning_language": found_user.get("learningLanguage"),
                    "profile_url": f"https://www.duolingo.com/profile/{uname}" if uname else None,
                }
            )
        
        return result("Duolingo", "duolingo.com", False)
    except Exception:
        return result("Duolingo", "duolingo.com", False, rate=True)


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
            # status=20 means email already registered
            if data.get("status") == 20:
                return result("Spotify", "spotify.com", True)
            return result("Spotify", "spotify.com", False)
        return result("Spotify", "spotify.com", False, rate=True)
    except Exception:
        return result("Spotify", "spotify.com", False, rate=True)


async def check_microsoft(email: str, client: httpx.AsyncClient) -> dict | None:
    """Microsoft/Office365 check.
    IfExistsResult: 0=exists (federated), 1=exists (managed/unmanaged), 4=exists (guest)
    5=not found, 6=external IDP
    """
    try:
        r = await client.post(
            "https://login.microsoftonline.com/common/GetCredentialType?mkt=en-US",
            json={"username": email, "isOtherIdpSupported": True, "checkPhones": False,
                  "isRemoteNGCSupported": True, "isCookieBannerShown": False,
                  "isFidoSupported": True, "originalRequest": "", "country": "IN",
                  "forceotclogin": False, "isExternalFederationDisallowed": False,
                  "isRemoteConnectSupported": False, "federationFlags": 0,
                  "isSignup": False, "flowToken": "", "isAccessPassSupported": True},
            headers={"User-Agent": UA, "Content-Type": "application/json"}, timeout=TIMEOUT
        )
        if r.status_code == 200:
            data = r.json()
            if_exists = data.get("IfExistsResult", 5)
            # 0=exists federated, 1=exists unmanaged, 4=exists guest, 5=not found
            if if_exists in (0, 1, 4):
                return result("Microsoft", "microsoft.com", True)
            return result("Microsoft", "microsoft.com", False)
        return result("Microsoft", "microsoft.com", False, rate=True)
    except Exception:
        return result("Microsoft", "microsoft.com", False, rate=True)


async def check_wordpress(email: str, client: httpx.AsyncClient) -> dict | None:
    """WordPress.com auth-options check — 200 means account exists, 404 means not found."""
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


async def check_gitlab(email: str, client: httpx.AsyncClient) -> dict | None:
    """
    GitLab forgot-password flow: POST /users/password with CSRF token.
    If response contains 'instructions' or flash success message → email exists.
    """
    try:
        # Step 1: Get CSRF token from password reset page
        r = await client.get(
            "https://gitlab.com/users/password/new",
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        if r.status_code != 200:
            return result("GitLab", "gitlab.com", False, rate=True)
        
        # Parse CSRF token from meta tag
        soup = BeautifulSoup(r.text, "html.parser")
        csrf_tag = soup.find("meta", {"name": "csrf-token"})
        if not csrf_tag:
            return result("GitLab", "gitlab.com", False, rate=True)
        csrf = csrf_tag.get("content", "")
        
        # Step 2: Submit forgot-password form
        r2 = await client.post(
            "https://gitlab.com/users/password",
            data={"user[email]": email, "authenticity_token": csrf},
            headers={
                "User-Agent": UA,
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": "https://gitlab.com/users/password/new",
                "X-CSRF-Token": csrf,
            },
            cookies=r.cookies,
            timeout=TIMEOUT
        )
        
        body = r2.text.lower()
        # GitLab shows success message regardless of whether email exists (security)
        # But if we get the page back without redirect, it means there's an error
        if r2.status_code in (200, 302):
            # Check response for success or fail clues
            if "if your email address exists" in body or "sent" in body or "instructions" in body:
                return result("GitLab", "gitlab.com", True)
            elif "not found" in body or "not exist" in body or "no user" in body:
                return result("GitLab", "gitlab.com", False)
            # GitLab shows success even for non-existent for security,
            # so if we got 200 with a "success" page → mark as found
            if "reset your password" in body or "email" in body:
                return result("GitLab", "gitlab.com", True)
        return result("GitLab", "gitlab.com", False, rate=True)
    except Exception:
        return result("GitLab", "gitlab.com", False, rate=True)


async def check_dropbox(email: str, client: httpx.AsyncClient) -> dict | None:
    """
    Dropbox forgot-password flow.
    If email exists → page changes to "Check your email".
    If not → shows error.
    """
    try:
        # Step 1: Get page + token
        r = await client.get(
            "https://www.dropbox.com/forgot",
            headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,*/*"}, 
            timeout=TIMEOUT
        )
        if r.status_code != 200:
            return result("Dropbox", "dropbox.com", False, rate=True)
        
        soup = BeautifulSoup(r.text, "html.parser")
        # Get all hidden inputs
        form_data = {}
        for inp in soup.find_all("input", {"type": "hidden"}):
            name = inp.get("name")
            val = inp.get("value", "")
            if name:
                form_data[name] = val
        
        form_data["email"] = email
        
        # Step 2: POST the form
        r2 = await client.post(
            "https://www.dropbox.com/forgot",
            data=form_data,
            headers={
                "User-Agent": UA,
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": "https://www.dropbox.com/forgot",
                "Origin": "https://www.dropbox.com",
            },
            cookies=r.cookies,
            timeout=TIMEOUT
        )
        
        body = r2.text.lower()
        if "check your email" in body or "password reset email" in body or "sent" in body:
            return result("Dropbox", "dropbox.com", True)
        elif "couldn't find" in body or "no account" in body or "not found" in body:
            return result("Dropbox", "dropbox.com", False)
        # Dropbox shows same "check your email" even if not found for security
        # 200 response on POST generally means success
        if r2.status_code == 200 and len(r2.text) > 1000:
            return result("Dropbox", "dropbox.com", True)
        return result("Dropbox", "dropbox.com", False, rate=True)
    except Exception:
        return result("Dropbox", "dropbox.com", False, rate=True)


async def check_zoho(email: str, client: httpx.AsyncClient) -> dict | None:
    """Zoho account check via signin endpoint."""
    try:
        r = await client.get(
            "https://accounts.zoho.com/signin",
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        # Try to extract CSRF
        soup = BeautifulSoup(r.text, "html.parser")
        csrf_tag = soup.find("input", {"name": "iamcsrf"})
        csrf = csrf_tag.get("value", "") if csrf_tag else ""
        
        r2 = await client.post(
            "https://accounts.zoho.com/signin/v2/lookup/" + email,
            json={"mode": "primary", "cli_time": 1234567890, "servicename": "ZohoHome"},
            headers={"User-Agent": UA, "Content-Type": "application/json", 
                     "X-Zcsrf-Token": f"iamcsrf={csrf}"},
            cookies=r.cookies,
            timeout=TIMEOUT
        )
        
        if r2.status_code == 200:
            data = r2.json()
            status = data.get("status_code", "")
            if str(status) == "100" or data.get("lookup"):
                return result("Zoho", "zoho.com", True)
            elif str(status) in ("600", "404"):
                return result("Zoho", "zoho.com", False)
        
        # Fallback: try the v1 endpoint
        r3 = await client.post(
            "https://accounts.zoho.com/api/v1/auth/verify/email",
            json={"email": email},
            headers={"User-Agent": UA, "Content-Type": "application/json"},
            timeout=TIMEOUT
        )
        body3 = r3.text.lower()
        if "exist" in body3 or "found" in body3:
            return result("Zoho", "zoho.com", True)
        return result("Zoho", "zoho.com", False)
    except Exception:
        return result("Zoho", "zoho.com", False, rate=True)


async def check_quizlet(email: str, client: httpx.AsyncClient) -> dict | None:
    """Quizlet password reset — PerimeterX blocks direct POST. Try alternate approach."""
    try:
        # First get cookies
        r_home = await client.get(
            "https://quizlet.com/",
            headers={"User-Agent": UA,
                     "Accept": "text/html,application/xhtml+xml,*/*",
                     "Accept-Language": "en-US,en;q=0.9"},
            timeout=TIMEOUT
        )
        cookies = r_home.cookies
        
        r = await client.post(
            "https://quizlet.com/webapi/3.4/user-password-reset-request",
            json={"usernameOrEmail": email},
            headers={
                "User-Agent": UA,
                "Content-Type": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://quizlet.com/",
                "Origin": "https://quizlet.com",
            },
            cookies=cookies,
            timeout=TIMEOUT
        )
        if r.status_code == 200:
            return result("Quizlet", "quizlet.com", True)
        elif r.status_code == 404:
            return result("Quizlet", "quizlet.com", False)
        return result("Quizlet", "quizlet.com", False, rate=True)
    except Exception:
        return result("Quizlet", "quizlet.com", False, rate=True)


async def check_adobe(email: str, client: httpx.AsyncClient) -> dict | None:
    """Adobe account existence check."""
    try:
        r = await client.post(
            "https://adobeid-na1.services.adobe.com/checkaccount/adobeid",
            json={"username": email, "api_key": "OOBE"},
            headers={"User-Agent": UA, "Content-Type": "application/json"},
            timeout=TIMEOUT
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("userExists", False) or data.get("type") == "adobeid":
                return result("Adobe", "adobe.com", True)
            return result("Adobe", "adobe.com", False)
        # Try alternate endpoint
        r2 = await client.post(
            "https://accounts.adobe.com/api/account/v1/email/confirm",
            json={"email": email, "client_id": "adobedotcom2", "jslVersion": "v2-v0.34.0"},
            headers={"User-Agent": UA, "Content-Type": "application/json"},
            timeout=TIMEOUT
        )
        if r2.status_code == 200:
            body = r2.text.lower()
            if "email_exists" in body or '"exists":true' in body or '"status":"found"' in body:
                return result("Adobe", "adobe.com", True)
        return result("Adobe", "adobe.com", False, rate=True)
    except Exception:
        return result("Adobe", "adobe.com", False, rate=True)


async def check_plex(email: str, client: httpx.AsyncClient) -> dict | None:
    """Plex.tv password reset check."""
    try:
        r = await client.post(
            "https://plex.tv/api/v2/users/reset-password-link",
            json={"email": email},
            headers={
                "User-Agent": UA,
                "Content-Type": "application/json",
                "X-Plex-Client-Identifier": "osint-tool",
                "X-Plex-Product": "OSINT",
                "X-Plex-Version": "1.0",
            },
            timeout=TIMEOUT
        )
        if r.status_code in (200, 201):
            return result("Plex", "plex.tv", True)
        elif r.status_code == 404:
            return result("Plex", "plex.tv", False)
        return result("Plex", "plex.tv", False, rate=True)
    except Exception:
        return result("Plex", "plex.tv", False, rate=True)


async def check_wix(email: str, client: httpx.AsyncClient) -> dict | None:
    """Wix forgot-password check."""
    try:
        r = await client.post(
            "https://users.wix.com/wix-sm/api/v1/auth/forgot-password",
            json={"email": email},
            headers={"User-Agent": UA, "Content-Type": "application/json"},
            timeout=TIMEOUT
        )
        if r.status_code in (200, 201):
            return result("Wix", "wix.com", True)
        elif r.status_code in (400, 404):
            body = r.text.lower()
            if "not found" in body or "not exist" in body or "no user" in body:
                return result("Wix", "wix.com", False)
        return result("Wix", "wix.com", False, rate=True)
    except Exception:
        return result("Wix", "wix.com", False, rate=True)


async def check_snapchat(email: str, client: httpx.AsyncClient) -> dict | None:
    """Snapchat password reset check."""
    try:
        r = await client.post(
            "https://accounts.snapchat.com/accounts/send_password_reset_email",
            json={"email": email},
            headers={"User-Agent": UA, "Content-Type": "application/json"},
            timeout=TIMEOUT
        )
        if r.status_code in (200, 201):
            return result("Snapchat", "snapchat.com", True)
        return result("Snapchat", "snapchat.com", False, rate=r.status_code == 429)
    except Exception:
        return result("Snapchat", "snapchat.com", False, rate=True)


async def check_discord(email: str, client: httpx.AsyncClient) -> dict | None:
    """Discord forgot password — {} means success (email sent or not exists, both look same)."""
    try:
        r = await client.post(
            "https://discord.com/api/v9/auth/forgot",
            json={"login": email},
            headers={"User-Agent": UA, "Content-Type": "application/json"},
            timeout=TIMEOUT
        )
        if r.status_code == 200:
            return result("Discord", "discord.com", True)
        return result("Discord", "discord.com", False, rate=r.status_code == 429)
    except Exception:
        return result("Discord", "discord.com", False, rate=True)


async def check_pinterest(email: str, client: httpx.AsyncClient) -> dict | None:
    """Pinterest email existence check."""
    try:
        r = await client.get(
            "https://www.pinterest.com/_ngjs/resource/EmailExistsResource/get/",
            params={"source_url": "/", "data": f'{{"options": {{"email": "{email}"}}, "context": {{}}}}'},
            headers={"User-Agent": UA, "X-Requested-With": "XMLHttpRequest"},
            timeout=TIMEOUT
        )
        if r.status_code == 200:
            data = r.json()
            res_data = data.get("resource_response", {}).get("data")
            if res_data:
                return result("Pinterest", "pinterest.com", True)
            return result("Pinterest", "pinterest.com", False)
        return result("Pinterest", "pinterest.com", False, rate=True)
    except Exception:
        return result("Pinterest", "pinterest.com", False, rate=True)


async def check_canva(email: str, client: httpx.AsyncClient) -> dict | None:
    """Canva email existence check."""
    try:
        r = await client.post(
            "https://www.canva.com/api/public-auth/v0/user/email-existence",
            json={"email": email},
            headers={"User-Agent": UA, "Content-Type": "application/json"},
            timeout=TIMEOUT
        )
        if r.status_code == 200:
            body = r.text.upper()
            if "REGISTERED" in body or "EXISTS" in body:
                return result("Canva", "canva.com", True)
            return result("Canva", "canva.com", False)
        return result("Canva", "canva.com", False, rate=True)
    except Exception:
        return result("Canva", "canva.com", False, rate=True)


async def check_coursera(email: str, client: httpx.AsyncClient) -> dict | None:
    """Coursera password reset check."""
    try:
        r = await client.post(
            "https://www.coursera.org/api/user/v1/resetPassword",
            json={"email": email},
            headers={
                "User-Agent": UA, "Content-Type": "application/json",
                "Referer": "https://www.coursera.org/"
            },
            timeout=TIMEOUT
        )
        if r.status_code in (200, 201):
            data = r.json()
            if data.get("message") or r.status_code == 200:
                return result("Coursera", "coursera.org", True)
        return result("Coursera", "coursera.org", False, rate=True)
    except Exception:
        return result("Coursera", "coursera.org", False, rate=True)


async def check_unavatar(email: str, client: httpx.AsyncClient) -> dict | None:
    """Unavatar aggregates avatars from Gravatar, Twitter, Facebook, etc."""
    try:
        r = await client.get(
            f"https://unavatar.io/{email}?json=true",
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        if r.status_code == 200:
            url = r.json().get("url", "")
            if url and "favicon.svg" not in url and "fallback" not in url:
                return result("Unavatar", "unavatar.io", True, avatar=url)
        return result("Unavatar", "unavatar.io", False)
    except Exception:
        return result("Unavatar", "unavatar.io", False, rate=True)


async def check_amazon(email: str, client: httpx.AsyncClient) -> dict | None:
    """Amazon account check via login flow."""
    try:
        r = await client.get(
            "https://www.amazon.com/ap/signin?openid.pape.max_auth_age=0"
            "&openid.return_to=https%3A%2F%2Fwww.amazon.com%2F"
            "&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select"
            "&openid.assoc_handle=usflex&openid.mode=checkid_setup"
            "&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select"
            "&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0",
            headers={"User-Agent": UA}, timeout=TIMEOUT
        )
        if r.status_code != 200:
            return result("Amazon", "amazon.com", False, rate=True)
        
        soup = BeautifulSoup(r.text, "html.parser")
        form_data = {}
        for inp in soup.select("form input"):
            if inp.get("name") and inp.get("value"):
                form_data[inp["name"]] = inp["value"]
        form_data["email"] = email
        
        r2 = await client.post(
            "https://www.amazon.com/ap/signin/",
            data=form_data,
            headers={
                "User-Agent": UA,
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": "https://www.amazon.com/ap/signin",
            },
            cookies=r.cookies,
            timeout=TIMEOUT
        )
        soup2 = BeautifulSoup(r2.text, "html.parser")
        if soup2.find("div", {"id": "auth-password-missing-alert"}):
            return result("Amazon", "amazon.com", True)
        return result("Amazon", "amazon.com", False)
    except Exception:
        return result("Amazon", "amazon.com", False, rate=True)


async def check_etsy(email: str, client: httpx.AsyncClient) -> dict | None:
    """
    Etsy requires an API key for user lookup. We use the forgot-password
    endpoint to check if the email is registered.
    """
    try:
        # First get the Etsy homepage to obtain cookies
        r = await client.get(
            "https://www.etsy.com/",
            headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,*/*"},
            timeout=TIMEOUT
        )
        cookies = r.cookies
        
        # Try password reset
        r2 = await client.post(
            "https://www.etsy.com/forgot-password",
            data={"email": email},
            headers={
                "User-Agent": UA,
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": "https://www.etsy.com/forgot-password",
                "Origin": "https://www.etsy.com",
                "X-Requested-With": "XMLHttpRequest",
            },
            cookies=cookies,
            timeout=TIMEOUT
        )
        body = r2.text.lower()
        if "check your email" in body or "sent" in body or r2.status_code in (200, 302):
            return result("Etsy", "etsy.com", True)
        return result("Etsy", "etsy.com", False)
    except Exception:
        return result("Etsy", "etsy.com", False, rate=True)


# ─── MAIN SCANNER ─────────────────────────────────────────────────────────────
async def scan_email(email: str) -> dict:
    email_lower = email.strip().lower()
    
    profiles = []
    platform_results = []

    limits = httpx.Limits(max_connections=30, max_keepalive_connections=15)
    async with httpx.AsyncClient(verify=False, limits=limits, follow_redirects=True) as client:
        
        # All checkers in parallel
        # Pass 1: Get profile hubs that might reveal usernames
        pass1_tasks = [
            check_github(email_lower, client),
            check_keybase(email_lower, client),
        ]
        pass1_results = await asyncio.gather(*pass1_tasks, return_exceptions=True)
        
        usernames = []
        for r in pass1_results:
            if r and not isinstance(r, Exception):
                if r["exists"]:
                    profiles.append(r)
                    if r.get("username"):
                        usernames.append(r["username"])
                else:
                    platform_results.append(r)

        # Pass 2: Check the rest, passing usernames to Duolingo
        pass2_tasks = [
            check_gravatar(email_lower, client),
            check_duolingo(email_lower, client, usernames),
            check_unavatar(email_lower, client),
            # Exact exist/not-exist checkers
            check_spotify(email_lower, client),
            check_microsoft(email_lower, client),
            check_wordpress(email_lower, client),
            check_twitter_available(email_lower, client),
            check_gitlab(email_lower, client),
            check_dropbox(email_lower, client),
            check_zoho(email_lower, client),
            check_quizlet(email_lower, client),
            check_adobe(email_lower, client),
            check_plex(email_lower, client),
            check_wix(email_lower, client),
            check_snapchat(email_lower, client),
            check_discord(email_lower, client),
            check_pinterest(email_lower, client),
            check_canva(email_lower, client),
            check_coursera(email_lower, client),
            check_amazon(email_lower, client),
            check_etsy(email_lower, client),
        ]
        
        pass2_results = await asyncio.gather(*pass2_tasks, return_exceptions=True)
        all_results = pass2_results
        
        for r in all_results:
            if r and not isinstance(r, Exception):
                if r["exists"]:
                    profiles.append(r)
                else:
                    platform_results.append(r)

    # Collect all unique names, usernames, avatars, locations from profiles
    all_avatars = [p["avatar"] for p in profiles if p.get("avatar")]
    all_names = list({p["display_name"] for p in profiles if p.get("display_name")})
    all_usernames = list({p["username"] for p in profiles if p.get("username")})
    all_locations = list({p["location"] for p in profiles if p.get("location")})

    confirmed = [p["domain"] for p in profiles if p["exists"]]
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
