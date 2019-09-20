# --- !Ups
CREATE TABLE garmadon_yarn_app_heuristic_result
(
    id                 INT(11)             NOT NULL AUTO_INCREMENT COMMENT 'The application heuristic result id',
    yarn_app_result_id VARCHAR(50)         NOT NULL COMMENT 'The application id',
    heuristic_class    VARCHAR(255)        NOT NULL COMMENT 'Name of the JVM class that implements this heuristic',
    heuristic_name     VARCHAR(128)        NOT NULL COMMENT 'The heuristic name',
    severity           TINYINT(2) UNSIGNED NOT NULL COMMENT 'The heuristic severity ranging from 0(LOW) to 4(CRITICAL)',
    score              MEDIUMINT(9) UNSIGNED        DEFAULT 0 COMMENT 'The heuristic score for the application. score = severity * number_of_tasks(map/reduce) where severity not in [0,1], otherwise score = 0',
    ready              BIT                 NOT NULL DEFAULT 0 COMMENT 'Indicate if it is ready to be inserted in dr-elephant heuristics',
    read_times         TINYINT(2) UNSIGNED          DEFAULT 0 COMMENT 'Number of time the value has been read',

    PRIMARY KEY (id)
);

CREATE TABLE garmadon_yarn_app_heuristic_result_details
(
    yarn_app_heuristic_result_id INT(11)      NOT NULL COMMENT 'The application heuristic result id',
    name                         VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'The analysis detail entry name/key',
    value                        VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'The analysis detail value corresponding to the name',
    details                      TEXT COMMENT 'More information on analysis details. e.g, stacktrace',

    PRIMARY KEY (yarn_app_heuristic_result_id, name),
    CONSTRAINT garmadon_yarn_app_heuristic_result_details_f1 FOREIGN KEY (yarn_app_heuristic_result_id) REFERENCES garmadon_yarn_app_heuristic_result (id)
);

CREATE TABLE garmadon_heuristic_help
(
    heuristic_id VARCHAR(255) NOT NULL COMMENT 'The heuristic help id',
    help_html    TEXT         NOT NULL COMMENT 'HTML content of the help',
    PRIMARY KEY (heuristic_id)
);