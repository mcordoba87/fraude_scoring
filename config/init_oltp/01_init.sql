-- ============================================================
-- Esquema OLTP inicial: users y transactions
-- Ruta: config/sql/init_oltp/01_schema.sql
-- Se ejecuta al inicializar el volumen de postgres_oltp
-- ============================================================

CREATE TABLE users (
    id                BIGSERIAL PRIMARY KEY,
    nombre            TEXT        NOT NULL,
    scoring_crediticio INT        NOT NULL CHECK (scoring_crediticio BETWEEN 300 AND 850),
    limite_credito    NUMERIC(15,2) NOT NULL,
    status_riesgo    TEXT        NOT NULL DEFAULT 'bajo',
    location         TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE transactions (
    id               BIGSERIAL PRIMARY KEY,
    user_id          BIGINT      NOT NULL REFERENCES users(id),
    amount           NUMERIC(15,2) NOT NULL,
    merchant_category TEXT        NOT NULL,
    location         TEXT,
    is_flagged_fraud BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_transactions_user_id    ON transactions (user_id);
CREATE INDEX idx_transactions_created_at ON transactions (created_at);
CREATE INDEX idx_users_status_riesgo     ON users (status_riesgo);