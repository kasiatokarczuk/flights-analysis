// Wykonywany raz, przy pierwszym starcie pustej bazy (katalog /docker-entrypoint-initdb.d).
// Tworzy użytkownika aplikacji, który może czytać i zapisywać tylko bazę projektu.
// Administrator (MONGO_INITDB_ROOT_USERNAME) służy wyłącznie do zarządzania serwerem.
db.getSiblingDB(process.env.MONGO_DATABASE).createUser({
  user: process.env.MONGO_APP_USER,
  pwd: process.env.MONGO_APP_PASSWORD,
  roles: [{ role: "readWrite", db: process.env.MONGO_DATABASE }],
});
