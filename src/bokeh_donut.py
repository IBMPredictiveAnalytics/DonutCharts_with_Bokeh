import pandas as pd
import bokeh
from bokeh.charts import output_file, Donut
from bokeh.io import show, save
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext

import sys

if len(sys.argv) > 1 and sys.argv[1] == "-test":
    import os
    df = pd.read_csv("Datasets/DRUG1N.csv")
    cat_field_1 = "Drug"
    cat_field_2 = "Cholesterol"
    value_field = ""
    output_option = 'output_to_screen'
    output_path = '/tmp/foo.html'
    output_width = 1024
    output_height = 1024
    output_font_size = 16
    title_font_size = 32
    title = "Test"
else:
    import spss.pyspark.runtime
    ascontext = spss.pyspark.runtime.getContext()
    sc = ascontext.getSparkContext()
    sqlCtx = ascontext.getSparkSQLContext()
    df = ascontext.getSparkInputData().toPandas()
    cat_field_1 = '%%inner_field%%'
    cat_field_2 = '%%outer_field%%'
    value_field = '%%value_field%%'
    output_option = '%%output_option%%'
    output_path = '%%output_path%%'
    output_width = int('%%output_width%%')
    output_height = int('%%output_height%%')
    output_font_size = int('%%output_font_size%%')
    title_font_size = int('%%title_font_size%%')
    title = '%%title%%'

majVersion = int(bokeh.__version__.split(".")[0])
minVersion = int(bokeh.__version__.split(".")[1])

if minVersion < 12 and majVersion == 0:
    raise Exception("This extension only works with Bokeh v0.12 or higher, you may want to try installing the latest version of the anaconda distribution")

if not value_field:
    value_field = '__value__'
    df[value_field] = 1

from bokeh.models import HoverTool

hover = HoverTool(
    tooltips=[
        ("Total", "@values")
    ]
)

labels = [cat_field_1]
if cat_field_2:
    labels.append(cat_field_2)

donut_from_df = Donut(df, label = labels,xlabel="X",ylabel="y",
                      values=value_field,
                      hover_text=value_field,
                      agg='sum',
                      width=output_width,
                      height=output_height,
                      text_font_size=str(output_font_size)+"pt",
                      title=title,
                      title_text_font_size=str(title_font_size)+"pt",
                      level_spacing=[0.0, 0.01],
                      tools=[hover])

# d = donut_from_df._builders[0].chart_data.data
# print(str(d))

if output_option == 'output_to_file':
    if not output_path:
        raise Exception("No output path specified")
else:
    from os import tempnam
    output_path = tempnam()

output_file(output_path, mode="inline")

if output_option == 'output_to_screen':
    show(donut_from_df)
    print("Output should open in a browser window")
else:
    save(donut_from_df)
    print("Output should be saved on the server to path: "+output_path)

