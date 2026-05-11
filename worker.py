from app import app, run_worker


if __name__ == "__main__":
    with app.app_context():
        run_worker()
