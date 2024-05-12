CREATE
    OR REPLACE PROCEDURE add_P2P_checks(
    checkingNickname VARCHAR(40),
    reviewerNickname VARCHAR(40),
    taskTitle VARCHAR,
    statusP2P INT,
    reviewTime TIME
)
    LANGUAGE plpgsql
AS
$procedure$
BEGIN
    IF (statusP2P = 0) THEN
        INSERT INTO checks(id, peer, task, date)
        VALUES ((SELECT MAX(id) + 1
                 FROM checks),
                reviewerNickname,
                taskTitle,
                CURRENT_DATE);

        INSERT INTO p2p(id, checks, checkingpeer, status, time)
        VALUES ((SELECT MAX(id) + 1
                 FROM P2P),
                (SELECT MAX(id)
                 FROM checks),
                checkingNickname,
                statusP2P,
                reviewTime);
    ELSE
        INSERT INTO p2p(id, checks, checkingpeer, status, time)
        VALUES ((SELECT MAX(id) + 1
                 FROM P2P),
                (SELECT checks.id
                 FROM checks
                          JOIN p2p on checks.id = p2p.checks AND
                                      checks.task = taskTitle AND
                                      checks.peer = reviewerNickname
                 WHERE p2p.status = 0
                   AND p2p.checkingpeer = checkingNickname),
                checkingNickname,
                statusP2P,
                reviewTime);
    END IF;
END
$procedure$;

CALL add_P2P_checks('rickieca',
                    'eloiskam',
                    'C8',
                    2,
                    LOCALTIME(0)
     );

CREATE
    OR REPLACE PROCEDURE add_to_Verter(
    checkingNickname VARCHAR(40),
    taskTitle VARCHAR,
    statusVerter INT,
    reviewTime TIME
)
    LANGUAGE plpgsql
AS
$procedure$
BEGIN
    INSERT INTO verter(id, checks, status, time)
    VALUES ((SELECT MAX(id) + 1
             FROM verter),
            (WITH temp_huemp AS (SELECT checks.id AS check_id,
                                        p2p.id    AS p2p_id
                                 FROM checks
                                          JOIN public.p2p on checks.id = p2p.checks
                                     AND status = 1
                                     AND checkingpeer = checkingNickname
                                     AND task = taskTitle)
             SELECT check_id
             FROM temp_huemp
             WHERE p2p_id = (SELECT MAX(p2p_id)
                             FROM temp_huemp)),
            statusVerter,
            reviewTime);
END
$procedure$;

CREATE OR REPLACE FUNCTION fnc_trg_change_transferred_points() RETURNS TRIGGER
    LANGUAGE plpgsql
AS
$function$
DECLARE
    reviewed_peer VARCHAR(40);
BEGIN
    IF new.status = 0 THEN
        reviewed_peer := (SELECT DISTINCT checks.peer
                          FROM p2p
                                   JOIN checks
                                        ON p2p.checks = checks.id
                          WHERE checkingpeer = new.checkingpeer);
        UPDATE transferredpoints
        SET pointsamount = (pointsamount + 1)
        WHERE checkingpeer = new.checkingpeer
          AND checkedpeer = reviewed_peer;
    END IF;
    RETURN NULL;
END
$function$;

CREATE TRIGGER trg_change_transferred_points
    AFTER INSERT
    ON p2p
    FOR EACH ROW
EXECUTE FUNCTION
    fnc_trg_change_transferred_points();

CREATE OR REPLACE FUNCTION fnc_trg_check_xp_data() RETURNS TRIGGER
    LANGUAGE plpgsql
AS
$function$
BEGIN
    IF new.checks <> (SELECT id
                      FROM checks
                      WHERE id = new.checks)
    THEN
        RAISE EXCEPTION 'Check doesnt exist...';
    END IF;
    IF new.xpamount > (SELECT maxxp
                      FROM checks
                               JOIN tasks
                                    ON checks.task = tasks.title
                      WHERE NEW.checks = checks.id)
    THEN
        RAISE EXCEPTION 'XP doesnt exist';
    END IF;

    RETURN NEW;
END
$function$;

CREATE OR REPLACE TRIGGER trg_check_xp_data
    AFTER INSERT
    ON xp
    FOR EACH ROW
EXECUTE FUNCTION
    fnc_trg_check_xp_data();

