# install.packages("reticulate")
library(reticulate)

t 
#####################################
repl_python()

import pandas as pd
dirThis = 'C:\\Users\\chcww\\Downloads\\'
testHistory = pd.read_csv(dirThis + 'testHistory.csv')
k = 1
exit()
#####################################

py$k


library(rsconnect)
rsconnect::deployApp('C:/R1091/new/myIMDB.Rmd')










library(shiny)
library(plotly)
library(data.table)

data = data.table(x=1:10, y=sample(1:100, 10)) 
food_labels = sample(c("cheeseburger", "hamburger", "salad", "fries"), 10, replace=T)

get_plotly <- function() {
  ggplotly(ggplot(data,aes(x,y)) + geom_point() + geom_text(label=food_labels))
}

ui <- fluidPage(
  plotlyOutput("plot"),
  verbatimTextOutput(outputId = "clicked_point")
)

server <- function(input, output, session) {
  
  plt <- reactive(get_plotly())
  
  output$plot <- renderPlotly(plt())
  
  s <- reactive(event_data("plotly_click"))
  
  label_name <- eventReactive(s(), {
    data.table(
      x = plt()$x$data[[2]]$x,
      y = plt()$x$data[[2]]$y,
      text = plt()$x$data[[2]]$text
    )[x==s()$x & y==s()$y, text]
  })
  
  output$clicked_point = renderPrint(label_name())
}

shinyApp(ui, server)




