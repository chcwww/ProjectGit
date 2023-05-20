library(shiny)
library(flexdashboard)
library(shinydashboard)
library(shinyMobile)
library(shinythemes)
library(data.table)
library(dplyr)
library(data.table)
library(DT)
library(shinymanager)
library(gapminder)
library(ggplot2)
library(gganimate)
library(shinyWidgets)
chooseSliderSkin("Modern")

options(shiny.maxRequestSize=600*1024^2)
pacman::p_load(tidyverse,highcharter,FactoMineR,plotly)
pacman::p_load(flexdashboard)
library(data.table)
library(DT)
# fread('C:/Users/chcww/Downloads/use.csv') -> K
# D1 <- K[sample(1:dim(K)[1], 100000),]
# write.csv(D1, "newuse.csv", row.names=FALSE)

fread('realusee.csv') -> K
D1 <- K
D1[,'month'] <- paste0(substr(D1$offerdate, 6, 7),"mon")

genres = D1$offer %>% unlist %>% table %>% sort(decr=T)
gnames = names(genres)


ggPCA = function(pca, dim=1:2, col="black", k=0.5, b=1.05) {
  vx = pca$var$coord[,dim] %>% as_tibble %>% 
    setNames(c("x","y")) %>% mutate(var = rownames(pca$var$coord))
  dx = pca$ind$coord[,dim] %>% as_tibble %>% setNames(c("x","y")) %>% 
    mutate(cos2=rowSums(pca$ind$cos2[,dim]) %>% as.numeric())
  a = k*max(abs(dx[,1:2]))/max(abs(vx[,1:2]))
  vx = mutate_at(vx, vars(x,y), `*`, a )
  vx2 = mutate_at(vx, vars(x,y), `*`, 0) %>% rbind(vx)
  vx = mutate_at(vx, vars(x,y), `*`, b )
  labs = paste0("Dim",dim," : ",round(pca$eig[dim,2],1),"%") 
  gp = ggplot() + 
    geom_text(data=vx,aes(x, y, label=var)) + 
    geom_path(data=vx2, aes(x, y, group=var), color=col) +
    xlab(labs[1]) + ylab(labs[2])
  list(gplot=gp, data=dx)
}



ui<-dashboardPage(
  
  dashboardHeader(title = em(h3(strong('Independent study🎓🎓'))),
                  titleWidth = 300),
  dashboardSidebar(width = 300,
                   sidebarMenu(
                     menuItem(strong("About"), tabName = 'homepage', icon = icon('home')),
                     menuItem(strong('Examples in chc'), tabName = 'gallery', icon = icon('book'),
                              menuSubItem(p('Data Table'), tabName = 'Dtreat'),
                              menuSubItem(p('Descriptive Statistics'), tabName = 'Ggexample')),
                     menuItem(strong("PCA Interactive Plot"), tabName = 'pca', icon = icon('rocket')),
                     menuItem(strong("Upload File"), tabName = 'upload', icon = icon('ghost'))
                   )),
  dashboardBody(
    
    tabItems(    
      tabItem(tabName = 'homepage',
              fluidPage(
                box(
                  title = h3(strong('👥Group 11')),
                  h3('Acquire Valued Shoppers🛒🛒'),
                  width = 200,
                  column(
                    imageOutput("plot1"),
                    width = 5
                  ),
                  column(
                    h3(strong('🖋Resume')),
                    p('National Taipei University'),
                    p('E-mail: chcwww1@gmail.com'),
                    h3(strong('📖Department')),
                    a(h4("NTPU STAT"),href = "https://www.stat.ntpu.edu.tw/"),
                    h3(strong('🤵Advisor')),
                    a(h4("王鴻龍"),href = "http://www.aacsb.ntpu.edu.tw/teach/teachsta.php?teacher=NDY3ODBf546L6bS76b6N"),
                    width = 7
                  )
                )
              )
      ),
      tabItem(tabName = 'Ggexample',
              fluidPage(
                
                box(title = 'Customers Numbers of Offers',
                    status = 'primary',
                    verbatimTextOutput('Plot'),
                    width = 10
                ),
                
                box(title = 'Summary of Choosen Columns (select at Data Table)',
                    status = 'primary',
                    verbatimTextOutput('summ'),
                    width = 10
                ),
                
                box(title = 'Offers Provided Every Month',
                    status = 'warning',
                    plotOutput('mont'),
                    width = 10,
                    solidHeader = TRUE
                )
              )
      ),
      tabItem(tabName = 'Dtreat',
              fluidPage(
                selectInput("col", "Select Columns", 
                            colnames(D1), colnames(D1)[1:7], multiple=T),
                HTML("<center>"),
                h3(strong("資料表")),
                HTML("</center>"),
                DTOutput("tData")
              )
      ),
      
      tabItem(tabName = 'pca',
              fluidPage(
                column(
                  selectInput("offer", "Select offer", 
                              unique(D1$offer), unique(D1$offer)[4:6], multiple=T),
                  sliderInput("amountTotal", "amount total",
                              0, 
                              summary(D1$buy_total_amount)[[5]] %>% round, 
                              c(round(summary(D1$buy_total_amount)[[2]]), 
                                round(summary(D1$buy_total_amount)[[5]])), 100),
                  
                  sliderInput("freqTotal", "freq total",
                              0, 
                              summary(D1$buy_total_freq)[[5]] %>% round, 
                              c(round(summary(D1$buy_total_freq)[[2]]), 
                                round(summary(D1$buy_total_freq)[[5]])), 100),
                  
                  sliderInput("daydiffTotal", "daydiff total",
                              0, 
                              summary(D1$buy_total_daydiff)[[5]] %>% round, 
                              c(round(summary(D1$buy_total_daydiff)[[2]]), 
                                round(summary(D1$buy_total_daydiff)[[5]])), 1),
                  
                  sliderInput("avgTotal", "CT total",
                              0, 
                              summary(D1$buy_total_CT)[[5]] %>% round, 
                              c(round(summary(D1$buy_total_CT)[[2]]), 
                                round(summary(D1$buy_total_CT)[[5]])), 10),
                  hr(),
                  sliderInput("cos2","Cos2 Mask", 0, 1, 0, 0.05),
                  width = 5
                ),
                column(
                  shinydashboard::valueBoxOutput("cart", width = 8),
                  br(),
                  br(),
                  br(),
                  br(),
                  br(),
                  br(),
                  br(),
                  plotly::plotlyOutput("ply"),
                  width = 7
                ), 
                
                column(tabsetPanel(type = "tabs",
                                   tabPanel("PCA Plot1", value = "tab1",
                                            plotly::plotlyOutput("pca1")
                                   ), 
                                   tabPanel("PCA Plot2", value = "tab2",
                                            plotly::plotlyOutput("pca2")
                                   )  
                ), width = 12
                )
              )
      ),
      
      
      tabItem(tabName = 'upload',
              fluidPage(
                sidebarLayout(
                  
                  # Sidebar panel for inputs ----
                  sidebarPanel(
                    
                    # Input: Select a file ----
                    fileInput("file1", "Choose CSV File",
                              multiple = TRUE,
                              accept = c("text/csv",
                                         "text/comma-separated-values,text/plain",
                                         ".csv")),
                    
                    tags$hr(),
                    
                    checkboxInput("header", "Header", TRUE),
                    
                    radioButtons("sep", "Separator",
                                 choices = c(Comma = ",",
                                             Semicolon = ";",
                                             Tab = "\t"),
                                 selected = ","),
                    
                    radioButtons("quote", "Quote",
                                 choices = c(None = "",
                                             "Double Quote" = '"',
                                             "Single Quote" = "'"),
                                 selected = '"'),
                    
                    hr(),
                    
                    radioButtons("disp", "Display",
                                 choices = c(Head = "head",
                                             All = "all"),
                                 selected = "head")
                    
                  ),
                  
                  mainPanel(
                    DTOutput("contents")
                    
                  )
                  
                )
              )
      )
      
      
      
    )
    
  ),
  
  skin = 'blue'
  
)


server<-function(input, output) {
  
  A = reactive({
    if(input$amountTotal[1] == 0) {
      k1 <- -9999999
    } else {
      k1 <- input$amountTotal[1]
    }
    
    if(input$freqTotal[1] == 0) {
      k2 <- -9999999
    } else {
      k2 <- input$freqTotal[1]
    }
    
    if(input$daydiffTotal[1] == 0){
      k3 <- -9999999
    } else {
      k3 <- input$daydiffTotal[1]
    }
    
    if(input$avgTotal[1] == 0){
      k4 <- -9999999 
    } else {
      k4 <- input$avgTotal[1]
    }
    
    
    
    
    if(input$amountTotal[2] == summary(D1$buy_total_amount)[[5]] %>% round) {
      t1 <- 99999999
    } else {
      t1 <- input$amountTotal[2]
    }
    
    if(input$freqTotal[2] == summary(D1$buy_total_freq)[[5]] %>% round) {
      t2 <- 99999999
    } else {
      t2 <- input$freqTotal[2]
    }
    
    if(input$daydiffTotal[2] == summary(D1$buy_total_daydiff)[[5]] %>% round){
      t3 <- 99999999
    } else {
      t3 <- input$daydiffTotal[2]
    }
    
    if(input$avgTotal[2] == summary(D1$buy_total_CT)[[5]] %>% round){
      t4 <- 99999999
    } else {
      t4 <- input$avgTotal[2]
    }
    
    D1 %>% filter(
      str_detect(offer, paste0(input$offer,collapse="|")),
      buy_total_amount >= k1, 
      buy_total_amount <= t1,
      buy_total_freq >= k2,
      buy_total_freq <= t2,
      buy_total_daydiff >= k3, 
      buy_total_daydiff <= t3,
      buy_total_CT >= k4,
      buy_total_CT <= t4,
    ) -> r.A  
  })
  
  
  output$cart <- shinydashboard::renderValueBox({
    shinydashboard::valueBox(
      "Customers",
      nrow(A()),
      icon = icon("cart-shopping")
    )
  }) 
  
  
  output$ply <- renderPlotly({
    
    df = as_tibble(genres) %>% setNames(c("offer", "All"))
    df = A()$offer %>% unlist %>% table %>% 
      as_tibble %>% setNames(c("offer", "Select")) %>% 
      right_join(df) %>% replace(is.na(.), 0) %>% 
      arrange(desc(All)) %>% 
      mutate(offer = factor(offer, rev(offer)))
    
    gg2 = df %>% gather("key","count",-1) %>%
      ggplot(aes(offer, count, fill=key)) +
      geom_col(position="identity",alpha=0.8) +
      coord_flip() + theme_bw() + labs(fill="") +
      scale_y_continuous(expand=c(0.02,0.02)) +
      theme(axis.title=element_blank(),
            axis.ticks=element_blank()); gg2
    
    ggplotly(gg2) %>% layout(
      legend=list(x=0.7, y=0.05),font=list(family="consolas"),
      margin=list(l=0, r=20, b=0, t=20, pad=4)
    )
    
  })
  
  
  GPC = reactive({
    select(A(), offervalue:buy_total_LT) %>% 
      PCA(graph=F) %>%
      ggPCA() -> r.GPC
  })
  
  
  output$pca1 <- renderPlotly({
    
    clrs = c("blue","seagreen","lightyellow","gold",
             "orange","magenta","red","darkred")
    
    gg = GPC()$gp + geom_point(
      data = cbind(A(), GPC()$data) %>% filter(cos2 >= input$cos2), 
      aes(x, y, label = id, size = buy_total_CT
          , color = buy_category_amount), 
      alpha=0.65) +
      scale_color_gradientn(colors=clrs) + theme_bw()
    
    ggplotly(gg) %>% config(scrollZoom=TRUE) %>% layout(
      dragmode="pan", font=list(family="consolas"))
    
  })
  
  
  output$pca2 <- renderPlotly({
    gg = GPC()$gp + geom_point(
      data = cbind(A(), GPC()$data) %>% filter(cos2 >= input$cos2), 
      aes(x, y, label = id, size = buy_total_CT
          , color = month), 
      alpha=0.65) + theme_bw()
    
    ggplotly(gg) %>% config(scrollZoom=TRUE) %>% layout(
      dragmode="pan", font=list(family="consolas"))
  })
  
  
  
  output$tData <- renderDT({
    datatable(select(D1, input$col), rownames = FALSE,
              options = list(autoWidth = TRUE))
  })
  
  
  output$Plot<-renderPrint({ 
    genres 
  })
  
  output$summ <- renderPrint({
    summary(select(D1, input$col))
  })
  
  output$mont <- renderPlot({ 
    paste0(substr(D1$offerdate, 6, 7),"月") %>% table %>% barplot
  })
  
  output$plot1 <- renderImage({
    
    # outfile <- tempfile(fileext='.gif')
    # 
    # 
    # p = ggplot(gapminder, aes(gdpPercap, lifeExp, size = pop, 
    #                           color = continent)) + geom_point() + scale_x_log10() +
    #   transition_time(year) + theme(axis.line=element_blank(),axis.text.x=element_blank(),
    #                                 axis.text.y=element_blank(),axis.ticks=element_blank(),
    #                                 axis.title.x=element_blank(),
    #                                 axis.title.y=element_blank(),legend.position="none",
    #                                 panel.background=element_blank(),panel.border=element_blank(),panel.grid.major=element_blank(),
    #                                 panel.grid.minor=element_blank(),plot.background=element_blank()) # New
    # 
    # anim_save("outfile.gif", animate(p))
    
    
    list(src = "outfile.gif",
         contentType = 'image/gif',
         width = 500,
         height = 400,
         alt = ""
    )}, deleteFile = FALSE)
  
  output$contents <- renderDT({
    req(input$file1)
    
    df1 <- read.csv(input$file1$datapath,
                    header = input$header,
                    sep = input$sep,
                    quote = input$quote)
    
    if(input$disp == "head") {
      datatable(head(df1))
    }
    else {
      datatable(df1)
    }
    
  })
  
}

shinyApp(ui = ui, server = server)