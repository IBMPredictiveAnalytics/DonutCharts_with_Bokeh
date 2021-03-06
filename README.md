# Donut Charts with Bokeh

This IBM SPSS Modeler extension enables Pie/Donut Charts to be generated using Python and Bokeh

Learn more about this implementation [from the Bokeh Documentation][4]

![Stream](https://raw.githubusercontent.com/IBMPredictiveAnalytics/DonutCharts_with_Bokeh/master/screenshots/stream.png)

---
Requirements
----
-	SPSS Modeler v18.0 or later
- [Python 2.7 Anaconda Distribution 4.1.1 or later](https://www.continuum.io/downloads)

More information here: [IBM Predictive Extensions][2]

---
Installation Instructions
----

#### Initial one-time set-up for PySpark Extensions

If using v18.0 of SPSS Modeler, navigate to the options.cfg file (Windows default path: C:\Program Files\IBM\SPSS\Modeler\18.0\config).  Open this file in a text editor and paste the following text at the bottom of the document:

  eas_pyspark_python_path, "*C:/Users/IBM_ADMIN/Anaconda/python.exe*"

  -   The italicized path should be replaced with the path to your python.exe from your Anaconda installation.

#### Extension Hub Installation
  1. Go to the Extension menu in Modeler and click "Extension Hub"
  2.	In the search bar, type the name of this extension and press enter
  3. Check the box next to "Get extension" and click OK at the bottom of the screen
  4. The extension will install and a pop-up will show what palette it was installed to

#### Manual Installation
  1.	[Save the .mpe file][3] to your computer
  2.	In Modeler, click the Extensions menu, then click Install Local Extension Bundle
  3.	Navigate to where the .mpe was saved and click open
  4.	The extension will install and a pop-up will show what palette it was installed

---
Example
----

[Download the example stream][5]
[Download the example data][6]

---
License
----

[Apache 2.0][1]

---
Contributors
----
- Niall McCarroll - ([www.mccarroll.net](http://www.mccarroll.net/))


[1]:http://www.apache.org/licenses/LICENSE-2.0.html
[2]:https://developer.ibm.com/predictiveanalytics/downloads
[3]:https://raw.githubusercontent.com/IBMPredictiveAnalytics/DonutCharts_with_Bokeh/master/BokehDonut.mpe
[4]:http://bokeh.pydata.org/en/latest/docs/reference/charts.html#donut
[5]:https://raw.githubusercontent.com/IBMPredictiveAnalytics/DonutCharts_with_Bokeh/master/example/example.str
[6]:https://raw.githubusercontent.com/IBMPredictiveAnalytics/DonutCharts_with_Bokeh/master/example/201402_station_data.csv
