USE ROLE ACCOUNTADMIN;
USE DATABASE NEW_TEST;
USE SCHEMA AUDIT;

-- Update existing secret with GitHub credentials
CREATE OR REPLACE SECRET github_secret
  TYPE = password
  USERNAME = 'Praveen946-PS'
  PASSWORD = 'BPraveen946$';

-- Create new API integration for GitHub
CREATE OR REPLACE API INTEGRATION github_api
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Praveen946-PS/')
  ALLOWED_AUTHENTICATION_SECRETS = (github_secret)
  ENABLED = TRUE;

-- Test the connection
CREATE OR REPLACE GIT REPOSITORY github_test
  API_INTEGRATION = github_api
  GIT_CREDENTIALS = github_secret
  ORIGIN = 'https://github.com/Praveen946-PS/Demo_SF.git';

ALTER GIT REPOSITORY github_test FETCH;

//https://github.com/Praveen946-PS/Demo_SF.git

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE API INTEGRATION github_api
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Praveen946-PS/')
  ALLOWED_AUTHENTICATION_SECRETS = (NEW_TEST.AUDIT.github_secret)
  ENABLED = TRUE;

  USE ROLE ACCOUNTADMIN;

SHOW API INTEGRATIONS LIKE 'GITHUB_API';