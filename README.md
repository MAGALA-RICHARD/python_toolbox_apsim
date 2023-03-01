# APSIM-python-toolbox
APSIM Next Generation Simulation python Toolbox
This Python toolbox allows users to easily upload and run simulations with the APSIM Next Generation software. With this tool, users can specify the start and end dates of the simulation, import a watershed feature class layer, and set the cell sampling resolution for the simulation. The tool will automatically download weather and soils data and incorporate them into the APSIM Next Generation file.
The tool leverages the multiprocessing module in Python to run the simulation in parallel, which significantly reduces the run time. The cell resolution has a significant impact on the run time, as it generates many samplings point profiles for the simulation.
This toolbox also has the capability to run multiple cropping systems simultaneously, allowing users to create simulation output patterns for multiple crops at once. Users have the ultimate freedom to specify whether they want to view the simulation in real-time by turning on the console or turning it off.
Features
•	Upload and run APSIM Next Generation simulations from Python
•	Specify start and end dates for the simulation
•	Import a watershed feature class layer
•	Set the cell sampling resolution for the simulation
•	Automatically download weather and soils data and incorporate them into the APSIM Next Generation file
•	Run the simulation in parallel using the multiprocessing module in Python
•	Run multiple cropping systems simultaneously
•	View simulation in real-time with the console
Requirements
•	Python 3.7 or higher
•	APSIM Next Generation software
•	Internet connection to download weather and soils data
•	ArcGIS PRO 3.0 or higher
Usage
1.	Download the toolbox and insert it in your desired directory
2.	Import the tool into your project folder in ARCGIS PRO
3.	Open the toolbox
4.	Specify the working directory. We recommend that you do not select networked folder that sync automatically with the internet this greatly interferes with the API.
5.	Specify the start and end dates, watershed feature class layer, and cell sampling resolution
6.	Select the cropping system you intend to simulate, continuous corn, corn/rye cover crops, corn-soybean rotation with or without cover crops. You can select all these cropping systems once and compare the results for each simulation
7.	Select the percentage of cores on your machine to be used in parallel processing. If you are not going to use the computer for other computing needs in real time, you can select higher percentage
8.	Optionally, set the console parameter to True to view the simulations in real-time.
9.	Press run.
10.	The simulation will run with continuous update in the ArcGIS toolbox message panel

