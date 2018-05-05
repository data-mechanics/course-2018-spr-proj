from app import app

if __name__ == '__main__':
    print("Running app on localhost port 5000...")
    app.debug = True
    app.run()
