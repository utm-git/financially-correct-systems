DROP TRIGGER IF EXISTS trigger_check_transaction_balance ON ledger_entries;
DROP FUNCTION IF EXISTS check_transaction_balance();

DROP TABLE IF EXISTS account_snapshots;
DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS accounts;

CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE transactions (
    id UUID PRIMARY KEY,
    reference VARCHAR(255) UNIQUE NOT NULL,
    description VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE ledger_entries (
    id UUID PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES transactions(id),
    account_id UUID NOT NULL REFERENCES accounts(id),
    amount NUMERIC(19, 4) NOT NULL, -- positive for credits, negative for debits
    sequence_num BIGSERIAL UNIQUE,  -- used for snapshot/replay ordering
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Enforce the balance invariant (sum of entries in a transaction must be 0)
CREATE OR REPLACE FUNCTION check_transaction_balance() RETURNS trigger AS $$
DECLARE
    total NUMERIC;
BEGIN
    SELECT SUM(amount) INTO total FROM ledger_entries WHERE transaction_id = NEW.transaction_id;
    -- If the total is not exactly 0, reject the transaction
    IF total != 0 THEN
        RAISE EXCEPTION 'Transaction % does not balance. Sum of entries is %', NEW.transaction_id, total;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger must be DEFERRED so that multiple inserts within the same transaction can complete before checking
CREATE CONSTRAINT TRIGGER trigger_check_transaction_balance
AFTER INSERT OR UPDATE ON ledger_entries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_transaction_balance();


CREATE TABLE account_snapshots (
    id UUID PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES accounts(id),
    balance NUMERIC(19, 4) NOT NULL,
    last_sequence_num BIGINT NOT NULL, -- points to ledger_entries.sequence_num at the time of snapshot
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
