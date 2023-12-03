-- Copyright 2023 Ververica Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  column_type_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE full_types
(
    id                   SERIAL,
    tiny_c               TINYINT,
    tiny_un_c            TINYINT UNSIGNED,
    tiny_un_z_c          TINYINT UNSIGNED ZEROFILL,
    small_c              SMALLINT,
    small_un_c           SMALLINT UNSIGNED,
    small_un_z_c         SMALLINT UNSIGNED ZEROFILL,
    medium_c             MEDIUMINT,
    medium_un_c          MEDIUMINT UNSIGNED,
    medium_un_z_c        MEDIUMINT UNSIGNED ZEROFILL,
    int_c                INTEGER,
    int_un_c             INTEGER UNSIGNED,
    int_un_z_c           INTEGER UNSIGNED ZEROFILL,
    int11_c              INT(11),
    big_c                BIGINT,
    big_un_c             BIGINT UNSIGNED,
    big_un_z_c           BIGINT UNSIGNED ZEROFILL,
    varchar_c            VARCHAR(255),
    char_c               CHAR(3),
    real_c               REAL,
    float_c              FLOAT,
    float_un_c           FLOAT UNSIGNED,
    float_un_z_c         FLOAT UNSIGNED ZEROFILL,
    double_c             DOUBLE,
    double_un_c          DOUBLE UNSIGNED,
    double_un_z_c        DOUBLE UNSIGNED ZEROFILL,
    decimal_c            DECIMAL(8, 4),
    decimal_un_c         DECIMAL(8, 4) UNSIGNED,
    decimal_un_z_c       DECIMAL(8, 4) UNSIGNED ZEROFILL,
    numeric_c            NUMERIC(6, 0),
    big_decimal_c        DECIMAL(65, 1),
    bit1_c               BIT,
    tiny1_c              TINYINT(1),
    boolean_c            BOOLEAN,
    PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO common_types
VALUES (DEFAULT, 127, 255, 255, 32767, 65535, 65535, 8388607, 16777215, 16777215, 2147483647,
        4294967295, 4294967295, 2147483647, 9223372036854775807,
        18446744073709551615, 18446744073709551615,
        'Hello World', 'abc', 123.102, 123.102, 123.103, 123.104, 404.4443, 404.4444, 404.4445,
        123.4567, 123.4568, 123.4569, 345.6, 34567892.1, 0, 1, true);