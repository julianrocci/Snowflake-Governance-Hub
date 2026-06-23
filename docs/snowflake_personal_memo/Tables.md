PERMANENT : critical data, always the need to go back.
Time-Travel 90days , Fail-Safe 7days

TRANSIENT : staging and shared intermediate tables which you know how to rebuild.
Time-Travel 1day , No Fail-Safe 

TEMPORARY : one-off work, disappears with the session.

EXTERNAL : data stored in an external data lake that you want to query from Snowflake.