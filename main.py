from churn_predication.pipeline.train_pipeline import TrainingPipeline



from flask import Flask

app = Flask(__name__)

@app.route("/train")
def train():
    train_pipeline = TrainingPipeline()
    if train_pipeline.is_pipeline_running:
        return 'running'
    train_pipeline.start()
    return 'trainig goof'
    

if __name__ == "__main__":
    
    app.run(debug=True,host='0.0.0.0',port=8080)