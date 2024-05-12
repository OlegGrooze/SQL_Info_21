CREATE OR REPLACE FUNCTION fnc_get_transferred_points() RETURNS
    TABLE (Peer1 VARCHAR, Peer2 VARCHAR, PointsAmounts INT)
LANGUAGE sql
AS
$function$
    WITH source1 AS (
            SELECT checkingpeer AS Peer1_table1,
                    checkedpeer AS Peer2_table1,
                    pointsamount AS pointsamount_table1
            FROM transferredpoints
                WHERE checkingpeer < checkedpeer),
        source2 AS (
            SELECT checkingpeer AS Peer2_table2,
                    checkedpeer AS Peer1_table2,
                    pointsamount * (-1) AS pointsamount_table2
            FROM transferredpoints
                WHERE checkingpeer >= checkedpeer),
        result AS (
            SELECT  Peer1_table1, Peer2_table1, pointsamount_table1 FROM source1
            UNION ALL
            SELECT  Peer1_table2, Peer2_table2, pointsamount_table2 FROM source2)
    SELECT Peer1_table1 AS Peer1, Peer2_table1 AS Peer2, SUM(pointsamount_table1) AS PointsAmounts
    FROM result
    GROUP BY Peer1, Peer2
    ORDER BY Peer1, Peer2;
$function$;

SELECT  *  FROM fnc_get_transferred_points();
DROP FUNCTION fnc_get_transferred_points();

CREATE OR REPLACE FUNCTION fnc_xp_completed_tasks() RETURNS
    TABLE (Peer VARCHAR, Task VARCHAR, XP INT)
LANGUAGE plpgsql
AS
$function$
BEGIN
    RETURN QUERY
        SELECT Checks.Peer AS Peer, Checks.Task AS Task, XPAmount AS XP
        FROM XP
        JOIN Checks ON Checks.ID = XP.Checks
        ORDER BY Peer, XP DESC;
END
$function$;

SELECT  *  FROM fnc_xp_completed_tasks();

CREATE OR REPLACE FUNCTION fnc_count_leaves(leaving_date DATE) RETURNS
    TABLE (Peer_name VARCHAR)
LANGUAGE plpgsql
AS
$function$
BEGIN
    RETURN Query
        WITH leaves AS (
            SELECT peer AS peer_leaving, count(state) AS num
            FROM timetracking
            WHERE state = 2 AND date = leaving_date
            GROUP BY peer_leaving
        )
        SELECT peer_leaving
        FROM leaves
WHERE num = 1;
END
$function$;

SELECT * FROM fnc_count_leaves('2022-05-25');

CREATE OR REPLACE FUNCTION fnc_calc_of_transferred_points() RETURNS
    TABLE (Peer VARCHAR, PointsChange NUMERIC)
LANGUAGE plpgsql
AS
$function$
BEGIN
    RETURN QUERY
    WITH source1 AS (
            SELECT CheckingPeer AS Peer_table1, SUM(PointsAmount) AS points
            FROM TransferredPoints
            GROUP BY Peer_table1),
        source2 AS (
            SELECT CheckedPeer AS Peer_table2, -SUM(PointsAmount) AS points
            FROM TransferredPoints
            GROUP BY Peer_table2),
        result AS (
            SELECT  Peer_table1, points FROM source1
            UNION ALL
            SELECT  Peer_table2, points FROM source2)
    SELECT Peer_table1, SUM(points) AS PointsChange
    FROM result
    GROUP BY Peer_table1
    ORDER BY
        PointsChange DESC;
END;
$function$;

SELECT * FROM fnc_calc_of_transferred_points();

CREATE OR REPLACE FUNCTION fnc_calc_of_transferred_points_from_task1() RETURNS
    TABLE (Peer VARCHAR, PointsChange NUMERIC)
LANGUAGE plpgsql
AS
$function$
BEGIN
    RETURN QUERY
    WITH source1 AS (
            SELECT Peer1 AS Peer_table1, SUM(PointsAmounts) AS points
            FROM fnc_get_transferred_points()
            GROUP BY Peer_table1),
        source2 AS (
            SELECT Peer2 AS Peer_table2, -SUM(PointsAmounts) AS points
            FROM fnc_get_transferred_points()
            GROUP BY Peer_table2),
        result AS (
            SELECT  Peer_table1, points FROM source1
            UNION ALL
            SELECT  Peer_table2, points FROM source2)
    SELECT Peer_table1, SUM(points) AS PointsChange
    FROM result
    GROUP BY Peer_table1;
END;
$function$;

SELECT * FROM fnc_calc_of_transferred_points_from_task1();

CREATE OR REPLACE FUNCTION fnc_most_checked_task() RETURNS
    TABLE(Dates DATE, Tasks VARCHAR)
LANGUAGE plpgsql
AS
$function$
BEGIN
	RETURN QUERY
	SELECT sorted.Date, sorted.Task
	FROM (
		SELECT CheckDate AS Date, TaskName AS Task,
		ROW_NUMBER() OVER (PARTITION BY CheckDate ORDER BY TaskCount DESC) AS row_num
		FROM(
			SELECT Date AS CheckDate, Task AS TaskName, COUNT(*)
			AS TaskCount
			FROM Checks
			GROUP BY CheckDate, Task
		) AS temp_table
	)AS sorted
	WHERE row_num = 1;
END;
$function$;

SELECT * FROM fnc_most_checked_task();

CREATE OR REPLACE FUNCTION fnc_peer_completed_cluster(project_type VARCHAR, bibaboba INT) RETURNS
    TABLE(Peer VARCHAR, Date DATE)
LANGUAGE plpgsql
AS
$function$
DECLARE
    max_project_name VARCHAR;
BEGIN
    max_project_name := (SELECT MAX(tasks.title) FROM tasks WHERE title LIKE project_type || '_');
    RETURN QUERY
    SELECT checks.peer, MAX(checks.date) AS date
    FROM tasks
        JOIN checks ON tasks.title = checks.task
        JOIN verter ON checks.id = verter.checks
    WHERE title = max_project_name AND verter.status = 1
    GROUP BY checks.peer;
    bibaboba := 3;
END;
$function$;

SELECT * FROM fnc_peer_completed_cluster('CPP');

CREATE OR REPLACE PROCEDURE prc_peer_statistics_on_project_clusters(cluster_name_1 VARCHAR, cluster_name_2 VARCHAR)
LANGUAGE plpgsql
AS
$procedure$
DECLARE
    count_peers_all NUMERIC;
    count_peers_cluster_1 NUMERIC;
    count_peers_cluster_2 NUMERIC;
    count_peers_both NUMERIC;
    count_peers_none NUMERIC;
BEGIN
    count_peers_all := (SELECT COUNT(DISTINCT Peer)
                        FROM Checks);
    count_peers_cluster_1 := (SELECT COUNT(DISTINCT Peer)
                              FROM Checks
                              WHERE Task LIKE cluster_name_1 || '_');
    count_peers_cluster_2 := (SELECT COUNT(DISTINCT Peer)
                              FROM Checks
                              WHERE Task LIKE cluster_name_2 || '_');
    count_peers_both := (SELECT COUNT(DISTINCT Peer)
                         FROM Checks
                         WHERE Task LIKE cluster_name_1 || '_' AND
                               Peer IN (SELECT DISTINCT Peer
                                        FROM Checks
                                        WHERE Task LIKE cluster_name_2 || '_'));
    count_peers_none := (SELECT COUNT(DISTINCT peer)
                         FROM checks
                         WHERE Peer NOT IN (SELECT DISTINCT Peer
                                            FROM Checks
                                            WHERE Task LIKE cluster_name_1 || '_') AND
                               Peer NOT IN (SELECT DISTINCT Peer
                                            FROM Checks
                                            WHERE Task LIKE cluster_name_2 || '_'));
    count_peers_cluster_1 := ROUND((count_peers_cluster_1 / count_peers_all) * 100, 2);
    count_peers_cluster_2 := ROUND((count_peers_cluster_2 / count_peers_all) * 100, 2);
    count_peers_both := ROUND((count_peers_both / count_peers_all) * 100, 2);
    count_peers_none := ROUND((count_peers_none / count_peers_all) * 100, 2);

    RAISE NOTICE 'Приступили только к блоку %: % %%', cluster_name_1, count_peers_cluster_1;
    RAISE NOTICE 'Приступили только к блоку %: % %%', cluster_name_2, count_peers_cluster_2;
    RAISE NOTICE 'Приступили только к двум блокам: % %%', count_peers_both;
    RAISE NOTICE 'Приступили только ни к одному блоку: % %%', count_peers_none;
END
$procedure$;

CALL prc_peer_statistics_on_project_clusters('C', 'CPP');

CREATE OR REPLACE PROCEDURE prc_peer_birthday_success_check()
LANGUAGE plpgsql
AS
$procedure$
DECLARE
    count_peers_all NUMERIC;
    count_peers_success NUMERIC;
    count_peers_unsuccess NUMERIC;
BEGIN
    count_peers_all := (SELECT
                            COUNT(DISTINCT Peer)
                        FROM
                            Checks);
    count_peers_success := (SELECT
                                COUNT(DISTINCT Peer)
                            FROM
                                peers JOIN checks ON peers.nickname = checks.peer
                                      JOIN p2p ON checks.id = p2p.checks
                            WHERE
                                status = 1 AND
                                extract(MONTH FROM date) = extract(MONTH FROM birthday) AND
                                extract(DAY FROM date) = extract(DAY FROM birthday));
    count_peers_unsuccess := (SELECT
                                COUNT(DISTINCT Peer)
                            FROM
                                peers JOIN checks ON peers.nickname = checks.peer
                                      JOIN p2p ON checks.id = p2p.checks
                            WHERE
                                status = 2 AND
                                extract(MONTH FROM date) = extract(MONTH FROM birthday) AND
                                extract(DAY FROM date) = extract(DAY FROM birthday));
    count_peers_success := ROUND((count_peers_success / count_peers_all) * 100, 2);
    count_peers_unsuccess := ROUND((count_peers_unsuccess / count_peers_all) * 100, 2);
    RAISE NOTICE 'Успешно сдали проект в свой день рождения: % %%', count_peers_success;
    RAISE NOTICE 'Неуспешно сдали проект в свой день рождения: % %%', count_peers_unsuccess;
END
$procedure$;

CALL prc_peer_birthday_success_check();

CREATE OR REPLACE PROCEDURE prc_find_peers_with_some_tasks(
    task_name_1 VARCHAR,
    task_name_2 VARCHAR,
    task_name_3 VARCHAR,
    cr_finding_peers REFCURSOR = 'cr_finding_peers'
)
AS $$
BEGIN
    OPEN cr_finding_peers FOR
        WITH peers_1st_and_2nd_task AS (
            SELECT 
                Checks.Peer
            FROM 
                Checks
                    JOIN P2P ON Checks.ID = P2P.checks
                    JOIN Verter ON Checks.ID = Verter.checks
            WHERE 
                Checks.Task = task_name_1 AND 
                  P2P.status = 1 AND 
                  Verter.status = 1
            INTERSECT ALL
            SELECT
                Checks.Peer
            FROM
                Checks
                JOIN P2P ON Checks.ID = P2P.checks
                JOIN Verter ON Checks.ID = Verter.checks
            WHERE
                Checks.Task = task_name_2 AND
                P2P.status = 1 AND
                Verter.status = 1
        ),
        peers_3rd_task AS (
            SELECT
                Checks.Peer
            FROM
                Checks
                JOIN P2P ON Checks.ID = P2P.checks
                JOIN Verter ON Checks.ID = Verter.checks
            WHERE Checks.Task = task_name_3 AND
                  P2P.status = 1 AND
                  Verter.status = 1
        )
        SELECT DISTINCT
            peers_1st_and_2nd_task.Peer
        FROM
            peers_1st_and_2nd_task
        WHERE
            peers_1st_and_2nd_task.Peer NOT IN (SELECT
                                            Peer
                                        FROM
                                            peers_3rd_task);
END;
$$ LANGUAGE plpgsql;


BEGIN;
	CALL prc_find_peers_with_some_tasks('C8', 'C2', 'C5');
	FETCH ALL cr_finding_peers;
COMMIT;


CREATE OR REPLACE PROCEDURE prc_recursive_tasks(
    cr_tasks REFCURSOR = 'cr_tasks'
)
AS $$
BEGIN
    OPEN cr_tasks FOR
        WITH RECURSIVE recursion AS (
            SELECT
                title AS task1,
                0 AS CountC
            FROM
                tasks
            UNION ALL
            SELECT
                tasks.title,
                recursion.CountC + 1
            FROM
                recursion
                JOIN tasks ON recursion.task1 = tasks.parenttask
        )
        SELECT
            task1 AS "Task",
            MAX(CountC) AS "PrevCount"
        FROM
            recursion
        GROUP BY
            task1
        ORDER BY
            2;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL prc_recursive_tasks();
    FETCH ALL cr_tasks;
COMMIT;

