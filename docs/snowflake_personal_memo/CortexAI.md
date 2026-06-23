SNOWFLAKE.CORTEX.SENTIMENT : -1 / 0 / 1

Analyse the sentiment of a customer
SNOWFLAKE.CORTEX.SENTIMENT(comment) as sentiment_score


SNOWFLAKE.CORTEX.SUMMARIZE

Summarize calls transcription, contracts, etc..
SNOWFLAKE.CORTEX.SUMMARIZE(contract_text) as TLDR


SNOWFLAKE.CORTEX.TRANSLATE

Translate language source to target language
SNOWFLAKE.CORTEX.TRANSLATE(original_text, 'auto', 'fr') as texte_en_francais


SNOWFLAKE.CORTEX.EXTRACT_ANSWER

You give text ( question) it answer
SNOWFLAKE.CORTEX.EXTRACT_ANSWER(incident_report, 'Quelle est la pièce défectueuse ?') as part_name