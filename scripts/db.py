"""Połączenia z bazami MongoDB: lokalną (kontener mongodb) i MongoDB Atlas.

    python scripts/db.py local   # sprawdź połączenie i zabezpieczenia lokalnej bazy
    python scripts/db.py atlas   # to samo dla Atlas

Dane logowania są w pliku .env (wzór: .env.example).
"""
import sys

from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError

from common import ROOT

TLS_CA = ROOT / "secrets" / "tls" / "ca.pem"
TARGETS = ["local", "atlas"]


def load_env():
    """Czyta plik .env w formacie KLUCZ=wartość (puste linie i # komentarze są pomijane)."""
    path = ROOT / ".env"
    if not path.exists():
        raise RuntimeError("Brak pliku .env. Skopiuj .env.example jako .env i wpisz hasła.")
    env = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip()
    return env


def client_for(target, env, **options):
    """Tworzy klienta MongoDB. Lokalnie: TLS z naszym CA i użytkownik aplikacji."""
    if target == "local":
        return MongoClient("localhost", 27017, tls=True, tlsCAFile=str(TLS_CA),
                           username=env["MONGO_APP_USER"], password=env["MONGO_APP_PASSWORD"],
                           authSource=env["MONGO_DATABASE"], serverSelectionTimeoutMS=5000, **options)
    # Adres mongodb+srv:// z Atlas zawiera użytkownika i hasło; TLS jest w nim włączony domyślnie.
    return MongoClient(env["ATLAS_URI"], serverSelectionTimeoutMS=10000, **options)


def connect(target):
    """Zwraca bazę projektu w wybranej bazie MongoDB (sprawdza połączenie od razu)."""
    env = load_env()
    client = client_for(target, env)
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError:
        hint = ("Czy kontener działa? Sprawdź: docker compose ps mongodb" if target == "local" else
                "Sprawdź ATLAS_URI w .env i czy Twój adres IP jest dodany w Atlas (Network Access).")
        raise RuntimeError(f"Brak połączenia z bazą {target}. {hint}")
    return client[env["MONGO_DATABASE"]]


def check(target):
    """Sprawdza połączenie oraz to, że bez szyfrowania i bez hasła nie da się dostać do danych."""
    env = load_env()
    database = connect(target)
    status = database.command("connectionStatus")["authInfo"]["authenticatedUsers"]
    print(f"{target}: połączono, MongoDB {database.client.server_info()['version']}, "
          f"użytkownik {status[0]['user']}, baza {database.name}")
    if target == "local":
        try:
            MongoClient("localhost", 27017, tls=False, serverSelectionTimeoutMS=3000).admin.command("ping")
            print("UWAGA: serwer przyjął połączenie bez TLS!")
        except PyMongoError:
            print("local: połączenie bez TLS odrzucone - poprawnie")
        try:
            client_for(target, {**env, "MONGO_APP_PASSWORD": "zle-haslo"}).admin.command("ping")
            print("UWAGA: serwer przyjął złe hasło!")
        except PyMongoError:
            print("local: logowanie złym hasłem odrzucone - poprawnie")
    else:
        # Klient ma kontekst SSL tylko wtedy, gdy połączenie jest szyfrowane.
        encrypted = database.client.options.pool_options._ssl_context is not None
        print(f"atlas: szyfrowanie TLS: {'tak' if encrypted else 'NIE'}")


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in TARGETS:
        sys.exit("Użycie: python scripts/db.py local|atlas")
    try:
        check(sys.argv[1])
    except (RuntimeError, KeyError, PyMongoError) as error:
        sys.exit(f"BŁĄD: {error}")
