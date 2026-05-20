CREATE TYPE member_status AS ENUM ('current', 'lapsed');

ALTER TABLE members
    ADD COLUMN member_status member_status NOT NULL DEFAULT 'current';
