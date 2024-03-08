import time
import psycopg2
import streamlit as st

@st.cache_data

def fetch_voting_stats():
    conn = psycopg2.connect("host = localhost dbname = voting user = postgres password = postgres")
    cur = conn.cursor()

    # Fetch total number of voters
    cur.execute(""" Select Count(*) as voters_count from voters """)
    voters_count = cur.fetchone()[0]

    # Fetch total number of candidates
    cur.execute(""" Select Count(*) as candidates_count from candidates """)
    candidates_count = cur.fetchone()[0]

    return voters_count, candidates_count


def update_data():
    last_refresh = st.empty()
    last_refresh.text(f" Last refresh at: {time.strftime('%Y-%m-%d %H:%M:%S')} ")

    # Fetch voting stats from postgres
    voters_count, candidates_count = fetch_voting_stats()

    # Display the statistics
    st.markdown("""-----""")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)

    consumer = create_kafka_consumer(topic_name)

st.title("Realtime Election Voting Dashboard.")
topic_name = "aggregated_votes_per_candidate"

update_data()    