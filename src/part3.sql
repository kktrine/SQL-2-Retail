CREATE ROLE administrator LOGIN PASSWORD '123';
GRANT SELECT,
    INSERT,
    UPDATE,
    DELETE ON ALL TABLES IN SCHEMA public TO administrator;
GRANT pg_signal_backend TO administrator;
CREATE ROLE visitor LOGIN PASSWORD '123';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO visitor;
-- Проверка через postgres что такие роли появились
SELECT *
FROM information_schema.role_table_grants
WHERE grantee IN (
        SELECT rolname
        FROM pg_roles
    )
    and (
        grantee = 'visitor'
        or grantee = 'administrator'
    )
SELECT *
FROM pg_roles
WHERE rolname = 'visitor'
    or rolname = 'administrator';
-- check PRIVILEGES
-- admin ok all operations
-- visitor ok only SELECT
SELECT *
FROM cards;
UPDATE cards
SET Customer_ID = 5
WHERE Customer_Card_ID = 1;
UPDATE cards
SET Customer_ID = 19
WHERE Customer_Card_ID = 1;
INSERT INTO cards
VALUES (23, 5);
DELETE FROM Cards
WHERE Customer_Card_ID = 23;