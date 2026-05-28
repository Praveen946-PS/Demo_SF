USE ROLE ACCOUNTADMIN;

-- Get latest from GitHub
ALTER GIT REPOSITORY NEW_TEST.AUDIT.github_test FETCH;

-- See the files
LS @NEW_TEST.AUDIT.github_test/branches/main/;

-- Read a specific file's contents
SELECT $1 FROM @NEW_TEST.AUDIT.github_test/branches/main/test4.sql;
