#import dependencies
import numpy as np
import datetime as dt
from datetime import datetime
from datetime import date
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
#create an engine for the hawaii.sqlite database
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#prevents the jsonify from sorting based on alphabetical order
app.config['JSON_SORT_KEYS'] = False

#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    """List all available api routes."""
    return (
        f"Climate App<br/>"
        f"<br/>"
        f"Available Routes:<br/>"
        f"<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"<br/>"
        f"For the following routes please put dates in yyyy-mm-dd format.<br/>"
        f"Replace start with your starting date.<br/>"
        f"Replace end with your end date.<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start/end<br/>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all dates and precipitation (pcrp)"""
    # Query all precipiations 
    precipitation = session.query(Measurement.date, Measurement.prcp).group_by(Measurement.date).all() 

    session.close()
    
    #converts query into dataframe 
    pre = pd.DataFrame(precipitation, columns=['date', 'pcrp'])

    #sets the index as the date and replaces nan with empty string
    pre.set_index('date', inplace=True)
    pre.fillna('none', inplace=True)
    
    #converts dataframe to dictionary with data being the key and pcrp as the value
    pr = pre.to_dict('index')

    return jsonify(pr)
    

@app.route("/api/v1.0/stations")
def stations():
    
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    """Return a list of all stations"""
    
    # Query all stations
    
    stations = session.query(Station.id, Station.station, Station.name).distinct().all()
                       
    session.close()
    
    #converts keyed tuple into dictionary
    station = [r._asdict() for r in stations]
    
    return jsonify(station)

@app.route("/api/v1.0/tobs")
def tobs():
    
    """Return a list of the TOBS of the most active station in the last year"""
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # finds the last date, converts it to datetime and then minuses a year from it
    
    last_date = session.query(Measurement.date, Measurement.prcp).order_by(Measurement.date.desc()).first()
    last_date_convert = dt.datetime.strptime(last_date[0], '%Y-%m-%d')
    query_date = last_date_convert - dt.timedelta(days=365)
    
    #query the most active station in the final year
    station_active1 = session.query(Measurement.station).\
                    filter(Measurement.date >= query_date).\
                    group_by(Measurement.station).distinct().\
                    order_by(func.count(Measurement.station).desc()).first()
    
    #collects the name of the station for filtering purposes
    station_name = station_active1[0]
    
    #query to find the TOBS of the most active station found in the above query
    station_2016 = session.query(Measurement.date, Measurement.tobs, Measurement.station) .\
                        filter(Measurement.date >= query_date) .\
                        filter(Measurement.station == station_name) .\
                        group_by(Measurement.date) .\
                        order_by(Measurement.date.desc()).all()  
    
    session.close()
    
    #converts keyed tuple into dictionary
    stat_act = [r._asdict() for r in station_2016]
    
    return jsonify(stat_act)

@app.route("/api/v1.0/<start>")
def start(start):
    
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    """Return a list of the min, max and avg TOBS of all dates greater than and equal to the start date"""
    
    #converts the start date entered into a string to be used in the query
    start_date = str(start)
    
    #query to find the min, avg and max TOBS of all dates greater than and equal to the start date
    start_search = session.query(Measurement.date, func.min(Measurement.tobs).label("min"), func.avg(Measurement.tobs).label("avg"), func.max(Measurement.tobs).label("max")).\
                filter(Measurement.date >= start_date).\
                group_by(Measurement.date).all()
    
    session.close()
    
    #converts keyed tuple into dictionary
    strt_search = [r._asdict() for r in start_search]
    
    return jsonify(strt_search)

@app.route("/api/v1.0/<start>/<end>")
def startend(start, end):
    
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    """Return a list of the min, max and avg TOBS between start and end date inclusive"""
    
    
    #converts the start and end date entered into strings to be used in the query
    start_date = str(start)
    end_date = str(end)
    
    #query to find the min, avg and max TOBS of the range chosen inclusive
    
    start_end = session.query(Measurement.date, func.min(Measurement.tobs).label("min"), func.avg(Measurement.tobs).label("avg"), func.max(Measurement.tobs).label("max")).\
                filter(Measurement.date >= start_date).\
                filter(Measurement.date <= end_date).\
                group_by(Measurement.date).all()
    
    session.close()
    
    #converts keyed tuple into dictionary
    strtend_search = [r._asdict() for r in start_end]
    
    return jsonify(strtend_search)


if __name__ == '__main__':
    app.run(debug=True)