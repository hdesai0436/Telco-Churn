from churn_predication.pipeline.train_pipeline import TrainingPipeline

def start_training(start=False):
    try:
        # if not start:
        #     return None
        print("Training Running")
        TrainingPipeline().start()
        
    except Exception as e:
        raise e 

if __name__ == '__main__':
    start_training()
