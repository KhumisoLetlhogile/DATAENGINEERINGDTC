#Setting the Base image we want to use
#We want to base our image on Python: 3.9
FROM python:3.9 
#Whatever we run after it will be based on Python 3.9 base image
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY ingest_data_homework.py ingest_data_homework.py

#We state what should be executed when we run the Python:3.9 image
ENTRYPOINT [ "python","ingest_data_homework.py" ]
