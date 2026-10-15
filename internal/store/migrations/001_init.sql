CREATE TABLE IF NOT EXISTS alerts (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  dedup_key       VARCHAR(64)     NOT NULL,
  rule            VARCHAR(32)     NOT NULL,
  severity        TINYINT         NOT NULL,
  title           VARCHAR(255)    NOT NULL,
  details         LONGTEXT        NOT NULL,
  player_id       BIGINT          NOT NULL DEFAULT 0,
  player_name     VARCHAR(64)     NOT NULL DEFAULT '',
  item_hash       VARCHAR(16)     NOT NULL DEFAULT '',
  origin          VARCHAR(255)    NOT NULL DEFAULT '',
  occurrences     BIGINT UNSIGNED NOT NULL DEFAULT 1,
  status          VARCHAR(16)     NOT NULL DEFAULT 'open',
  generation      INT UNSIGNED    NOT NULL DEFAULT 0,
  first_seen      DATETIME(3)     NOT NULL,
  last_seen       DATETIME(3)     NOT NULL,
  resolved_at     DATETIME(3)     NULL,
  resolved_by     VARCHAR(64)     NOT NULL DEFAULT '',
  resolution_note VARCHAR(500)    NOT NULL DEFAULT '',
  published_at    DATETIME(3)     NULL,
  created_at      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_alerts_dedup (dedup_key),
  KEY idx_alerts_status (status, severity, id),
  KEY idx_alerts_unpublished (published_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS checkpoints (
  name       VARCHAR(32)     NOT NULL,
  cursor_id  BIGINT UNSIGNED NOT NULL,
  rule_state LONGTEXT        NOT NULL,
  updated_at DATETIME(3)     NOT NULL,
  PRIMARY KEY (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
