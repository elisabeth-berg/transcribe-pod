from flask import Flask, request, render_template,jsonify
from episode_search import search_episodes

app = Flask(__name__)

def do_something(text1,text2):
   text1 = text1.upper()
   text2 = text2.upper()
   combine = text1 + text2
   return combine

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/search', methods=['GET', 'POST'])
def search():
    search_phrase = request.form['search_phrase']
    results = search_episodes(search_phrase)
    output = {
        "e{}".format(i):
        results.iloc[i]["filename"] + ":\n" + results.iloc[i]["context"]
        for i in range(len(results))
    }
    return jsonify(result=output)

if __name__ == '__main__':
    app.run(debug=True)
