CREATE DATABASE s21_school;

CREATE TABLE
    Peers (
        Nickname VARCHAR(40) PRIMARY KEY,
        Birthday DATE NOT NULL CHECK (Birthday > '1930-01-01')
    );

CREATE TABLE
    Tasks (
        Title VARCHAR PRIMARY KEY,
        ParentTask VARCHAR,
        MaxXP INT NOT NULL CHECK (MaxXP > 0)
    );

CREATE TABLE
    Checks (
        ID SERIAL PRIMARY KEY,
        Peer VARCHAR(40) NOT NULL,
        Task VARCHAR(40) NOT NULL,
        Date DATE,
        FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
        FOREIGN KEY (Task) REFERENCES Tasks (Title)
    );

CREATE TABLE
    XP (
        ID SERIAL PRIMARY KEY,
        Checks INT NOT NULL,
        XPAmount INT CHECK (XPAmount >= 0) DEFAULT 0,
        FOREIGN KEY (Checks) REFERENCES Checks (ID)
    );

CREATE TABLE
    Verter (
        ID SERIAL PRIMARY KEY,
        Checks INT NOT NULL,
        Status INT NOT NULL CHECK (
            Status >= 0
            AND Status <= 2
        ) DEFAULT 0,
        Time TIME NOT NULL,
        FOREIGN KEY (Checks) REFERENCES Checks (ID)
    );

CREATE TABLE
    TimeTracking (
        ID SERIAL PRIMARY KEY,
        Peer VARCHAR(40) NOT NULL,
        Date DATE NOT NULL,
        Time TIME NOT NULL,
        State INT NOT NULL CHECK (
            State >= 1
            AND State <= 2
        ),
        FOREIGN KEY (Peer) REFERENCES Peers (Nickname)
    );

CREATE TABLE
    Recommendations(
        ID SERIAL PRIMARY KEY,
        Peer VARCHAR(40) NOT NULL,
        RecommendedPeer VARCHAR(40) NOT NULL,
        FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
        FOREIGN KEY (RecommendedPeer) REFERENCES Peers (Nickname)
    );

CREATE TABLE
    Friends (
        ID SERIAL PRIMARY KEY,
        Peer1 VARCHAR(40) NOT NULL,
        Peer2 VARCHAR(40) NOT NULL,
        FOREIGN KEY (Peer1) REFERENCES Peers (Nickname),
        FOREIGN KEY (Peer2) REFERENCES Peers (Nickname),
		CONSTRAINT ct_peer1_is_not_equal_peer2 CHECK (peer1 <> peer2) 
    );

CREATE TABLE
    TransferredPoints (
        ID SERIAL PRIMARY KEY,
        CheckingPeer VARCHAR(40) NOT NULL,
        CheckedPeer VARCHAR(40) NOT NULL,
        PointsAmount INT NOT NULL CHECK (
            PointsAmount >= 1
            AND PointsAmount <= 11
        ),
        FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname),
        FOREIGN KEY (CheckedPeer) REFERENCES Peers (Nickname)
    );

CREATE TABLE
    P2P (
        ID SERIAL PRIMARY KEY,
        Checks INT NOT NULL,
        CheckingPeer VARCHAR(40) NOT NULL,
        Status INT NOT NULL CHECK (
            Status >= 0
            AND Status <= 2
        ) DEFAULT 0,
        Time TIME NOT NULL,
        FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname),
        FOREIGN KEY (Checks) REFERENCES Checks (ID)
    );

CREATE
OR REPLACE PROCEDURE import (IN table_name VARCHAR, IN directory VARCHAR) LANGUAGE plpgsql AS $procedure$
BEGIN
    EXECUTE format('COPY %s FROM ''%s'' WITH CSV HEADER;', table_name, directory);
END;
$procedure$;

CREATE
OR REPLACE PROCEDURE export (IN table_name VARCHAR, IN directory VARCHAR) LANGUAGE plpgsql AS $procedure$
BEGIN
    EXECUTE format('COPY %s TO ''%s'' WITH CSV HEADER;', table_name, directory);
END;
$procedure$;

CALL import (
    'peers',
    '/tmp/peers.csv'
);  

CALL import (
    'tasks',
    '/tmp/tasks.csv'
);

CALL import (
    'checks',
    '/tmp/checks.csv'
);

CALL import (
    'xp',
    '/tmp/xp.csv'
);

CALL import (
    'verter',
    '/tmp/verter.csv'
);

CALL import (
    'friends',
    '/tmp/friends.csv'
);

CALL import (
    'timetracking',
    '/tmp/time_tracking.csv'
);

CALL import (
    'Recommendations',
    '/tmp/recommendations.csv'
);

CALL import (
    'TransferredPoints',
    '/tmp/transferred_points.csv'
);

CALL import (
	'P2P',
	'/tmp/P2P.csv'
);