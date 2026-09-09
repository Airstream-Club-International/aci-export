-- Name of the bundled interest config a job maintains on its audience.
-- Null for a job whose audience carries no preference group.
alter table mailchimp add column interests text;
