"""
Rassegna Automotive Giornaliera - v0.5 (fonti IT+EU; rassegna + script video)
Pipeline: RSS ingest -> dedup/interleave equo -> sintesi AI fedele (Claude)
          -> aggiorna il Google Doc rassegna + invia email HTML
          -> genera gli script video del giorno e li archivia (con data) in un 2o Doc
Copertura a 360 gradi su tutto il settore automotive.
Un'unica esecuzione giornaliera (cron nel workflow); invia a ogni esecuzione.

Setup una tantum:
    pip install feedparser anthropic google-api-python-client google-auth
    Variabili d'ambiente:
      ANTHROPIC_API_KEY, GMAIL_USER, GMAIL_APP_PASSWORD, RECIPIENT (opz.)
      GDOC_ID, GOOGLE_SERVICE_ACCOUNT_JSON   (opz.: se assenti, salta il Doc rassegna)
      GDOC_SCRIPTS_ID   (opz.: se assente, salta la generazione degli script video)
"""
import os
import sys
import re
import json
import html
import smtplib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from email.mime.text import MIMEText
from urllib.parse import quote_plus
import feedparser
import anthropic

ROME = ZoneInfo("Europe/Rome")

# --- CONFIG: FONTI --------------------------------------------------------
# Helper: costruisce un feed RSS di Google News da una query (limitata a 24h).
def gn(query, lang="it", country="IT"):
    return (f"https://news.google.com/rss/search?q={quote_plus(query + ' when:1d')}"
            f"&hl={lang}&gl={country}&ceid={country}:{lang}")

# Helper: feed RSS di Google News ristretto a una singola testata (site:dominio).
def gsite(domain, lang="it", country="IT"):
    return gn(f"site:{domain}", lang, country)

# ===== FONTI ITALIANE (prima le italiane) =================================
IT_FEEDS = [
    # Feed RSS nativi, gia' verificati (restano nativi perche' funzionanti).
    "https://it.motor1.com/rss/news/all/",      # Motor1 Italia
    "https://www.autoblog.it/feed",             # Autoblog Italia
    "https://formulapassion.it/automoto/feed",  # FormulaPassion (auto + motorsport)
    # Altre testate italiane via Google News ristretto alla singola testata:
    gsite("quattroruote.it"),                   # generalista di riferimento
    gsite("alvolante.it"),                       # prove, listino nuovo/usato
    gsite("auto.it"),                            # news, prove, sezione green
    gsite("corriere.it/motori"),                # Corriere Motori
    gsite("ansa.it/canale_motori"),             # ANSA Motori (agenzia)
    gn("site:ilsole24ore.com auto"),            # Il Sole 24 Ore (taglio economico)
    gsite("fleetmagazine.com"),                 # noleggio & flotte (la tua nicchia)
    gn("site:missionline.it flotte"),           # MissionFleet (auto aziendali)
    gsite("autoaziendalimagazine.it"),          # Auto Aziendali Magazine
    gsite("autosprint.it"),                     # motorsport
    gsite("ruoteclassiche.it"),                 # auto d'epoca
]

# ===== FONTI EUROPEE (poi le europee) ====================================
EU_FEEDS = [
    # Industria & business
    gsite("europe.autonews.com", "en", "US"),   # Automotive News Europe
    gsite("atz-magazine.com", "en", "US"),      # ATZ (tecnologia / R&D)
    gsite("automotivelogistics.media", "en", "US"),
    # Flotte & mobility (la tua nicchia su scala europea)
    gsite("fleeteurope.com", "en", "US"),       # Fleet Europe
    gsite("fleetnews.co.uk", "en", "GB"),       # Fleet News (UK)
    gsite("globalfleet.com", "en", "US"),       # Global Fleet
    # Generaliste per Paese
    gsite("auto-motor-und-sport.de", "de", "DE"),
    gsite("autobild.de", "de", "DE"),
    gsite("autozeitung.de", "de", "DE"),
    gn("site:adac.de auto", "de", "DE"),        # ADAC Motorwelt
    gsite("autocar.co.uk", "en", "GB"),         # Autocar (dal 1895)
    gsite("autoexpress.co.uk", "en", "GB"),     # Auto Express
    gsite("evo.co.uk", "en", "GB"),             # EVO (alte prestazioni)
    # Elettrico
    gsite("electrek.co", "en", "US"),
    gsite("insideevs.com", "en", "US"),
    gsite("electrive.com", "en", "US"),
]

FEEDS = IT_FEEDS + EU_FEEDS   # ordine: prima le italiane, poi le europee

HOURS_LOOKBACK = 24
MAX_ARTICLES = 100
MODEL = "claude-haiku-4-5-20251001"     # verifica il nome su docs.claude.com

SECTIONS_ORDER = [
    "Mercato & industria",
    "Novità modelli & prove",
    "Elettrico, ibrido & motori",
    "Tecnologia & guida autonoma",
    "Motorsport",
    "Fisco, NLT & leasing",
    "Internazionale",
]

# Segreti da variabili d'ambiente (MAI scritti nel codice).
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
GMAIL_USER = os.environ["GMAIL_USER"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
RECIPIENT = os.environ.get("RECIPIENT", GMAIL_USER)

GDOC_ID = os.environ.get("GDOC_ID")
GOOGLE_SA_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

# --- Generazione script video (attiva solo se GDOC_SCRIPTS_ID e' impostato) --
GDOC_SCRIPTS_ID = os.environ.get("GDOC_SCRIPTS_ID")   # Doc-archivio degli script
SCRIPT_MODEL = "claude-sonnet-5"    # modello capace per la scrittura (verifica su docs.claude.com)
MAX_SCRIPTS = 5                     # quanti script generare al giorno

BRAND_CONTEXT = """Sei il content strategist di un personal brand automotive.
Posizionamento: "l'auto come decisione economica".
Pubblico: prosumer - appassionati, freelance, partite IVA, piccoli imprenditori.
Tono: competente ma accessibile, concreto, diretto, senza gergo inutile e senza hype.
Pilastri: 1) analisi costi; 2) prove pratiche dei modelli (per chi deve decidere);
3) myth-busting su noleggio a lungo termine, leasing e fisco; 4) dietro le quinte/personale."""


# NOTA: nessuna guardia oraria. Il workflow ha UN solo cron giornaliero, quindi
# lo script invia una volta sola a ogni esecuzione, anche se GitHub la avvia in
# ritardo. (In precedenza il controllo "solo alle 07:00" scartava le esecuzioni
# partite in ritardo, lasciandoti senza mail.)


# --- 1. RACCOLTA (una lista per feed, per poterle poi alternare) ----------
def real_source(entry, feed):
    src = entry.get("source")
    if isinstance(src, dict) and src.get("title"):
        return src["title"]
    title = entry.get("title", "")
    if " - " in title:
        return title.rsplit(" - ", 1)[-1].strip()
    return feed.feed.get("title", "Fonte")


def clean_title(entry):
    t = entry.get("title", "").strip()
    if " - " in t:
        t = t.rsplit(" - ", 1)[0].strip()
    return t


def fetch_by_feed():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=HOURS_LOOKBACK)
    buckets = []
    for url in FEEDS:
        try:
            feed = feedparser.parse(url)
        except Exception as exc:
            print(f"[warn] feed non raggiungibile: {url} ({exc})", file=sys.stderr)
            buckets.append([])
            continue
        bucket = []
        for e in feed.entries:
            published = None
            if getattr(e, "published_parsed", None):
                published = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
            if published and published < cutoff:
                continue
            title = clean_title(e)
            if not title:
                continue
            snippet = re.sub("<[^>]+>", "", e.get("summary", ""))[:300].strip()
            bucket.append({
                "title": title,
                "link": e.get("link", ""),
                "source": real_source(e, feed),
                "snippet": snippet,
            })
        buckets.append(bucket)
    return buckets


# --- 2. MERGE EQUO (round-robin tra i feed + dedup) -----------------------
def merge(buckets):
    """Alterna le fonti (una notizia a testa a rotazione) per una copertura
    bilanciata di tutto il settore, poi taglia a MAX_ARTICLES. Nessun tema
    viene privilegiato: conta solo freschezza + varieta' delle fonti."""
    seen, out = set(), []
    i = 0
    while any(i < len(b) for b in buckets):
        for bucket in buckets:
            if i < len(bucket):
                it = bucket[i]
                key = it["title"].lower()[:80]
                if key and key not in seen:
                    seen.add(key)
                    out.append(it)
        i += 1
    return out[:MAX_ARTICLES]


# --- 3. SINTESI AI FEDELE -------------------------------------------------
def summarize(items):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    catalog = {f"a{i}": it for i, it in enumerate(items)}
    listing = "\n".join(
        f'{aid}: {it["title"]}' + (f' — {it["snippet"]}' if it["snippet"] else "")
        for aid, it in catalog.items()
    )
    prompt = f"""Sei l'editor di una rassegna stampa automotive generalista, che copre
a 360 gradi tutto il settore: mercato, modelli, motori, tecnologia, sport e mobilità.

Hai un elenco di notizie (id: titolo — estratto). Devi:
1. Selezionare le notizie realmente rilevanti (scarta gossip, clickbait, doppioni).
2. Dare una COPERTURA EQUILIBRATA di tutto il settore: non privilegiare un singolo
   tema, spazia tra le varie aree in base a quello che offre la giornata.
3. Raggrupparle in queste sezioni: {", ".join(SECTIONS_ORDER)}.
   - Se ci sono notizie su fisco/NLT/leasing raccoglile nella loro sezione
     (interesse professionale del lettore), ma senza sacrificare l'ampiezza sul resto.
4. Per ogni notizia scrivere UNA frase di sintesi in italiano, FEDELE al contenuto:
   riassumi solo cio' che e' presente nel titolo/estratto, non inventare nulla.
5. Massimo 4 notizie per sezione, le piu' rilevanti. Ometti le sezioni vuote.

Rispondi SOLO con JSON valido, senza testo attorno ne' backtick, in questo formato:
{{"sections": [{{"title": "<una delle sezioni elencate>", "items": [{{"id": "aX", "summary": "<frase>"}}]}}]}}

NOTIZIE:
{listing}
"""
    msg = client.messages.create(
        model=MODEL,
        max_tokens=3000,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = "".join(b.text for b in msg.content if b.type == "text").strip()
    raw = re.sub(r"^```(?:json)?|```$", "", raw, flags=re.MULTILINE).strip()
    return json.loads(raw), catalog


def _ordered_sections(data, catalog):
    order = {t: i for i, t in enumerate(SECTIONS_ORDER)}
    sections = sorted(data.get("sections", []), key=lambda s: order.get(s.get("title"), 99))
    for sec in sections:
        rows = [it for it in sec.get("items", []) if it.get("id") in catalog]
        if rows:
            yield sec.get("title", ""), rows


# --- 4a. RENDER HTML (per la mail) ----------------------------------------
def render_html(data, catalog):
    oggi = datetime.now(ROME).strftime("%d/%m/%Y")
    parts = [
        '<div style="font-family:Arial,Helvetica,sans-serif;max-width:640px;'
        'margin:0 auto;color:#1a1a1a;line-height:1.5">'
        f'<h1 style="font-size:20px;border-bottom:2px solid #111;padding-bottom:8px">'
        f'Rassegna Automotive - {oggi}</h1>'
    ]
    for title, rows in _ordered_sections(data, catalog):
        parts.append(
            f'<h2 style="font-size:15px;color:#b30000;margin:18px 0 6px">'
            f'{html.escape(title)}</h2><ul style="padding-left:18px;margin:0">'
        )
        for it in rows:
            art = catalog[it["id"]]
            parts.append(
                f'<li style="margin-bottom:10px">{html.escape(it.get("summary","").strip())} '
                f'<a href="{html.escape(art["link"])}" '
                f'style="color:#0645ad;text-decoration:none">[{html.escape(art["source"])}]</a></li>'
            )
        parts.append("</ul>")
    parts.append(
        '<p style="font-size:11px;color:#888;margin-top:24px">'
        'Sintesi generata automaticamente; fonti sempre linkate. '
        "Verifica l'articolo originale prima di riutilizzare le informazioni.</p></div>"
    )
    return "\n".join(parts)


# --- 4b. RENDER TESTO/MARKDOWN (per il Google Doc del Progetto) -----------
def render_markdown(data, catalog):
    oggi = datetime.now(ROME).strftime("%d/%m/%Y")
    lines = [f"# Rassegna Automotive — {oggi}", ""]
    for title, rows in _ordered_sections(data, catalog):
        lines.append(f"## {title}")
        for it in rows:
            art = catalog[it["id"]]
            lines.append(f"- {it.get('summary','').strip()} "
                         f"(Fonte: {art['source']} — {art['link']})")
        lines.append("")
    lines.append("---")
    lines.append("Materiale grezzo per l'ideazione contenuti. "
                 "Verifica sempre l'articolo originale prima di pubblicare.")
    return "\n".join(lines)


# --- 5a. AGGIORNA GOOGLE DOC ----------------------------------------------
def update_google_doc(text):
    if not (GDOC_ID and GOOGLE_SA_JSON):
        print("Google Doc non configurato: salto l'aggiornamento.")
        return
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_SA_JSON),
        scopes=["https://www.googleapis.com/auth/documents"],
    )
    service = build("docs", "v1", credentials=creds, cache_discovery=False)
    doc = service.documents().get(documentId=GDOC_ID).execute()
    end_index = doc["body"]["content"][-1]["endIndex"]
    requests = []
    if end_index > 2:
        requests.append({"deleteContentRange":
                         {"range": {"startIndex": 1, "endIndex": end_index - 1}}})
    requests.append({"insertText": {"location": {"index": 1}, "text": text}})
    service.documents().batchUpdate(documentId=GDOC_ID, body={"requests": requests}).execute()


# --- 5b. INVIO EMAIL ------------------------------------------------------
def send_email(html_body):
    oggi = datetime.now(ROME).strftime("%d/%m/%Y")
    user = GMAIL_USER.strip()
    # rimuove ogni spazio dalla app password (anche i non-breaking \xa0 da copiaincolla)
    password = re.sub(r"\s+", "", GMAIL_APP_PASSWORD)
    msg = MIMEText(html_body, "html", "utf-8")
    msg["Subject"] = f"Rassegna Automotive - {oggi}"
    msg["From"] = user
    msg["To"] = RECIPIENT.strip()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(user, password)
        s.send_message(msg)


# --- 6. SCRIPT VIDEO (generazione + archiviazione datata) -----------------
def generate_scripts(data, catalog):
    """Genera script pronti da registrare dalle notizie curate della rassegna."""
    news = []
    for title, rows in _ordered_sections(data, catalog):
        for it in rows:
            art = catalog[it["id"]]
            news.append(f"[{title}] {it.get('summary','').strip()} "
                        f"(Fonte: {art['source']} - {art['link']})")
    if not news:
        return None
    listing = "\n".join(news)
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt = f"""{BRAND_CONTEXT}

Dalle notizie della rassegna di oggi (sotto), scegli le {MAX_SCRIPTS} con maggiore
potenziale di contenuto e, per ciascuna, scrivi uno script di video verticale
PRONTO DA REGISTRARE (stesso 9:16 per Instagram Reels, TikTok e YouTube Shorts).

Per ogni script includi:
- Notizia e pilastro (con link alla fonte)
- Hook (0-3 sec)
- Script parlato (20-45 sec, italiano parlato reale, battute brevi, un'idea alla volta)
- Chiusura + CTA da personal brand
- Testo a schermo (3-6 keyword)
- Idee visive / B-roll
- Caption + 5-8 hashtag
- Note per piattaforma (Reels / TikTok / Shorts)

Regole:
- Attieniti ai fatti della notizia: non inventare cifre o dichiarazioni non presenti.
- Dove servirebbe un dato fiscale/NLT preciso o una presa di posizione personale,
  scrivi [DA VERIFICARE] o [MIA OPINIONE] invece di inventare.
- Voce competente ma accessibile, niente hype. Output in italiano, in testo semplice.

NOTIZIE:
{listing}
"""
    msg = client.messages.create(
        model=SCRIPT_MODEL,
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )
    return "".join(b.text for b in msg.content if b.type == "text").strip()


def archive_scripts(text):
    """Inserisce il blocco di oggi IN CIMA al Doc-archivio, con la data,
    mantenendo sotto tutti i giorni precedenti (storico consultabile)."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    oggi = datetime.now(ROME).strftime("%d/%m/%Y")
    sep = "=" * 40
    block = f"{sep}\nSCRIPT VIDEO - {oggi}\n{sep}\n\n{text}\n\n\n"
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_SA_JSON),
        scopes=["https://www.googleapis.com/auth/documents"],
    )
    service = build("docs", "v1", credentials=creds, cache_discovery=False)
    # insertText all'indice 1 = in cima: lo storico esistente scorre sotto.
    service.documents().batchUpdate(
        documentId=GDOC_SCRIPTS_ID,
        body={"requests": [{"insertText": {"location": {"index": 1}, "text": block}}]},
    ).execute()


# --- MAIN -----------------------------------------------------------------
def main():
    articles = merge(fetch_by_feed())
    if not articles:
        print("Nessuna notizia rilevante nelle ultime 24h.")
        return
    try:
        data, catalog = summarize(articles)
    except Exception as exc:
        print(f"[error] sintesi AI fallita: {exc}", file=sys.stderr)
        sys.exit(1)

    errors = []
    try:
        update_google_doc(render_markdown(data, catalog))
        print("Google Doc aggiornato.")
    except Exception as exc:
        errors.append(f"Google Doc: {exc}")
    try:
        send_email(render_html(data, catalog))
        print("Email inviata.")
    except Exception as exc:
        errors.append(f"Email: {exc}")

    # Script video: solo se configurato (GDOC_SCRIPTS_ID). Non blocca la rassegna.
    if GDOC_SCRIPTS_ID and GOOGLE_SA_JSON:
        try:
            scripts = generate_scripts(data, catalog)
            if scripts:
                archive_scripts(scripts)
                print("Script video archiviati nel Doc.")
        except Exception as exc:
            errors.append(f"Script video: {exc}")

    if errors:
        for e in errors:
            print(f"[error] {e}", file=sys.stderr)
        sys.exit(1)
    print(f"Fatto - {len(articles)} notizie candidate.")


if __name__ == "__main__":
    main()
