import os, json, time
from pathlib import Path
from urllib import request, error

WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

ROOT = Path.home() / "policyqueue"
POSTS = ROOT / "posts"

def send(msg: str):
    payload = {"content": msg[:1900]}  # Discord limit safety
    data = json.dumps(payload).encode("utf-8")

    req = request.Request(
        WEBHOOK_URL,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "User-Agent": "PolicyQueue/1.0"
        }
    )

    with request.urlopen(req, timeout=15):
        pass

def main():
    if not WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL not set.")

    print("Discord notifier running.")
    send("🚀 PolicyQueue draft watcher online.")

    seen = set()

    while True:
        for f in POSTS.glob("*.post.txt"):
            if f.name not in seen:
                content = f.read_text(encoding="utf-8", errors="ignore")
                send(f"📝 Draft Ready:\n\n{content}")
                print(f"Sent: {f.name}")
                seen.add(f.name)

        time.sleep(10)

if __name__ == "__main__":
    main()
