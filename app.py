from flask import Flask, jsonify
import psycopg2
import os

app = Flask(__name__)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME")
    )

@app.route("/top-words")
def top_words():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT word, count FROM word_count ORDER BY count DESC LIMIT 10")
    results = cursor.fetchall()
    conn.close()
    return jsonify([{ "word": row[0], "count": row[1] } for row in results])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)