# GnanaPath
<h3> Graph-based framework for connected-data analytics</h3>


The framework allows you to store data into node and edges and creates edges as based on logic you can provide.
The framework has  visualization of metadata nodes and datanodes.

It is built using following 

- python-spark
- cytoscape-js
- jupyter notebook
- backend graph engine is using neo4j 
- python flask-based ui

The framework can be execute as part of docker containers

<h2>This project is still under construction Please check for update on this page </h2>


<h3> Using Gnanapath</h3>

We built using jupyter/pyspark notebook.  

- Download and install jupyter/pyspark notebook or get docker container from https://hub.docker.com/r/jupyter/pyspark-notebook/

- If you choose docker container for notebook, then make sure you port map for 5050 for Gnana UI
 Example:
  # docker run -d -p 8888:8888  -p 5050:5050  --name jupyntbook  jupyter/pyspark

- Open terminal on the notebook and git clone GnanaPath repo

 # git clone https://github.com/Datalyrix/GnanaPath.git

-  Install python related modules for this project

# pip install -r gnpappsrv/requirements.txt


- Start
