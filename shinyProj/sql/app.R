library(shiny)
library(RODBC) 
library(DT)
library(DBI)
library(odbc)

# UI
ui <- fluidPage(
  HTML('<center>'),
  textInput("bins", label = h3("請輸入指令:"), value = "top 5 *"),
  HTML('</center>'),
  actionButton("action", label = "查詢"),
  
  hr(),
  
  HTML('<center>'),
  h3("trianFULL 數量"),
  HTML('</center>'),
  DTOutput("DT_table"),
  
  hr(),
  
  HTML('<center>'),
  h3("trianFULL head"),
  HTML('</center>'),
  DTOutput("shiny_table")
)

# Server
server <- function(input, output) {
  
  observeEvent(input$action, {
    conn <- dbConnect(
      odbc(),
      Driver      = "FreeTDS",
      Database    = "chc",
      Uid         = "chc",
      Pwd         = "6F489bxk",
      Server      = "chc.database.windows.net",
      Port        = 1433,
      TDS_Version = 7.4
    )

    sql1 <- " SELECT count(*) FROM testFULL"
    sql2 <- paste0(" SELECT ", input$bins," FROM testFULL")
  
    fetch <- dbGetQuery(conn, sql1)
    data <- dbGetQuery(conn, sql2)
    
    # DT版
    output$DT_table <- renderDT({
      datatable(fetch)
    })
    
    # Shiny版
    output$shiny_table <- renderDT({
      datatable(data)
    })
  
  })
}

# Create Shiny app
shinyApp(ui = ui, server = server)