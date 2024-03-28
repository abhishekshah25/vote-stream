## Realtime Voting System

### System Components

1. main.py: This is the main Python script that creates the required tables on postgres (candidates, voters & votes), it also creates the Kafka topic and creates a copy of the votes table in the Kafka topic. It also contains the logic to consume the votes from the Kafka topic and produce data to voters_topic on Kafka.

2. voting.py: This is the Python script that contains the logic to consume the votes from the Kafka topic (voters_topic), generate voting data and produce data to votes_topic on Kafka.

3. spark-streaming.py: This is the Python script that contains the logic to consume the votes from the Kafka topic (votes_topic), enrich the data from postgres and aggregate the votes and produce data to specific topics on Kafka.

4. streamlit-app.py: This is the Python script that contains the logic to consume the aggregated voting data from the Kafka topic as well as postgres and display the data in realtime using Streamlit.
