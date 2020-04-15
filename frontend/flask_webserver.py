from flask import Flask
from flask_pymongo import PyMongo
from flask import render_template

# RUN THE APP
# Windows
# set FLASK_APP=flask_webserver.py
# flask run
#
# Linux
# export FLASK_APP=flask_webserver.py
# flask run

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://192.168.0.143:32768/Vault"
mongo = PyMongo(app)


@app.route("/adcp/<serial_number>")
def adcp_serial_page(serial_number):
    adcp = mongo.db.adcps.find_one_or_404({"SerialNumber": serial_number})
    print(adcp)
    return render_template("adcp.html", adcp=adcp)


@app.route("/")
def adcp_list_page():
    adcps = mongo.db.adcps.find().sort("created", -1)
    print(adcps)
    return render_template("adcp_list.html", adcps=adcps)


@app.route("/hydro")
def hydro_page():
    hydro = mongo.db.HydrophoneLakeTestResults.find({})
    print(hydro[0])
    return render_template("hydro.html", hydros=hydro[0])


@app.route("/hydro/<serial_number>")
def hydro_serial_page(serial_number):
    hydro = mongo.db.HydrophoneLakeTestResults.find_one_or_404({"SerialNumber": serial_number})
    print(hydro)
    return render_template("hydro.html", hydros=hydro)