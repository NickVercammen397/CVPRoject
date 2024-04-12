from pathlib import Path

import pandas as pd
import seaborn as sns


from shiny import reactive
from shiny.express import input, render, ui
import shinyswatch

from DatabaseControl.dataController import getFullDataframe

ui.page_opts(fillable=True)
ui.include_css(Path(__file__).parent / "style.css")
shinyswatch.theme.darkly()

#add legend and metadata?
df_investment = getFullDataframe("investment")
df_len_motorway = getFullDataframe("len_motorway")
def_len_road= getFullDataframe("len_road")
df_performance = getFullDataframe("performance")
datasets = ["Investment", "length  of Motorways","Lenght of state provincial and communal roads", "Performance of vehicles by category"]

with ui.sidebar():
    ui.input_select("datasets", "Available Datasets", datasets,selected="Education Level")
    @render.ui
    def renderVarCheckbox():
        selectedDataframe = getSelectedDataframe()
        return ui.input_checkbox_group("variables","Select Variables",selectedDataframe.columns.values.tolist())
    
@reactive.calc
def getSelectedDataframe() -> pd.DataFrame:
    select = input.datasets()
    if select == "Investment":
        return df_investment
    if select == "length  of Motorways":
        return df_len_motorway
    if select == "Lenght of state provincial and communal roads":
        return def_len_road
    if select == "Performance of vehicles by category":
        return df_performance

@reactive.calc
def getSelectedVariables() -> pd.DataFrame:
    return getSelectedDataframe()[list(input.variables())]

@render.ui
def renderheader():
    return ui.h1(input.datasets())
with ui.navset_pill(id="tab"):
        with ui.nav_panel("Legend"):
            ui.h2("Legend")
            @render.ui
            def renderLegend():
                legend= []
                for x in getSelectedVariables().attrs["legende"]:
                    legendHeader = x.split(":")[0] 
                    legend.append(ui.h3(legendHeader))
                    if len(x.split(":")) > 1:
                        for y in x.split(":")[1].split('/'):
                            legend.append(ui.p(y))
                return legend
            
        with ui.nav_panel("Data Grid"):
            with ui.layout_column_wrap(width=1,height="80vh"):
                @render.data_frame
                def summary_statistics():
                    filterDataframe = getSelectedVariables()
                    if len(filterDataframe.columns)>0:
                        return render.DataGrid(filterDataframe, width="80vw", filters=True)
        with ui.nav_panel("Plot"):
            with ui.layout_sidebar(height="80vh"):
                with ui.sidebar():
                    ui.h4("Select Plot Variables")
                    @render.ui
                    def renderPlotSelectX():
                        return ui.input_select("x","Select Plot X",getSelectedVariables().columns.values.tolist())
                    @render.ui
                    def renderPlotSelectY():
                        return ui.input_select("y","Select Plot Y",getSelectedVariables().columns.values.tolist())
                    ui.input_checkbox("groupingVar", "add Grouping Var", False)
                    @render.ui
                    def renderPlotSelectHue():
                        options = getSelectedVariables().columns.values.tolist()
                        options.append("None")
                        return ui.input_select("hue","Select Grouping Variable",options)
                @render.plot()
                def SelectedGroupbar():
                    if input.groupingVar() == False:
                        return sns.barplot(
                            data=getSelectedDataframe(),
                            x=input.x(),
                            y=input.y())
                    else:
                        return sns.barplot(
                            data=getSelectedDataframe(),
                            x=input.x(),
                            y=input.y(),                            
                            hue=input.hue()
                            )