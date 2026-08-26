-- A BRN follows the person, not the membership: a number stays with a member
-- whose membership has lapsed until it is reassigned, and leadership records
-- outlive membership. Point brns at users so any synced user can hold a number.
ALTER TABLE brns
    DROP CONSTRAINT brns_user_id_fkey;

ALTER TABLE brns
    ADD CONSTRAINT brns_user_id_fkey
    FOREIGN KEY (user_id)
    REFERENCES users(id);

-- One row per continuous tenure of a number by a user; end_date IS NULL marks
-- the current holder, which brns records too for lookups that only need today.
-- Numbers here need not appear in brns: a number with no current holder still
-- has a history. Only tenures of synced users are stored, so the spans for a
-- number do not necessarily account for its whole life.
CREATE TABLE brn_ownership (
    number text NOT NULL,
    user_id text NOT NULL REFERENCES users(id),
    start_date date NOT NULL,
    end_date date,
    PRIMARY KEY (number, user_id, start_date)
);

-- Lookups by number ride the primary key, which leads with it.
CREATE INDEX idx_brn_ownership_user_id ON brn_ownership(user_id);

ALTER TABLE brn_ownership ENABLE ROW LEVEL SECURITY;
