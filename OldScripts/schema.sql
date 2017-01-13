CREATE TABLE daily_pagecounts(
    date VARCHAR(25),
    title VARCHAR(100),
    num_entries INT,
    num_requests INT(10),
    PRIMARY KEY(date, title)
 );
