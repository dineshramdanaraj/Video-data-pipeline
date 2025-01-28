# Video-data-pipeline

Install Grapviz
Install Python
Install Kafka
Install Poetry


Docker Image - Airflow
Docker Image - Kafka??

Install all the packages via - Poetry (package manager)

env variables (customize according to the need)

run sync-datalake-files.py to create temp .ipynb files for testing purposes - "python run sync-datalake-files.py

Local Sink - Directory used for watching incoming video
Local Staging - Directory used for dumping derivative video

SMTPlib - used for messaging the users regarding pipeline updates

Graphviz - tool used to visualize DAG workflows within ipynb files

Hamilton - Used for micro level DAG for better modularity and testing 

Airflow - Used for Macro Level DAG

Kafka - Used as a prod-sub to send notification to the system on arrival of video files

Ruff - Python Linter used for organizing the code.

Architecture-
Meddalion Architecture - 3 layers of data loading/transformation
                        Bronze - to load the extracted data from producer
                        Silver - to process the video based on the task requirements
                        Gold- to dump the data into the database after processing and sends notification.

                    
