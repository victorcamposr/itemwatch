-- Minimal game schema used by the integration tests and the demo compose:
-- the ledger plus the item tables the duplicate scan reads.
CREATE TABLE IF NOT EXISTS players (
  id   INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS item_hash_log (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  hash        CHAR(16) NOT NULL,
  event       VARCHAR(16) NOT NULL,
  parent      CHAR(16) NOT NULL DEFAULT '',
  itemtype    INT UNSIGNED NOT NULL,
  count       INT UNSIGNED NOT NULL DEFAULT 1,
  origin      VARCHAR(100) NOT NULL DEFAULT '',
  source      VARCHAR(160) NOT NULL DEFAULT '',
  player_id   INT NOT NULL DEFAULT 0,
  created_at  BIGINT NOT NULL,
  short_lived TINYINT(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  KEY hash (hash),
  KEY created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS player_items (
  player_id INT NOT NULL,
  sid       INT NOT NULL,
  itemtype  INT NOT NULL,
  hash      CHAR(16) NOT NULL DEFAULT '',
  KEY hash (hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS player_depotitems LIKE player_items;
CREATE TABLE IF NOT EXISTS player_inboxitems LIKE player_items;
CREATE TABLE IF NOT EXISTS player_rewards LIKE player_items;

CREATE TABLE IF NOT EXISTS market_offers (
  id        INT NOT NULL AUTO_INCREMENT,
  player_id INT NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS market_offer_items (
  offer_id INT NOT NULL,
  hash     CHAR(16) NOT NULL DEFAULT '',
  KEY hash (hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
