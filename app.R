library(shiny)
library(shinydashboard)
library(leaflet)
library(leaflet.extras)
library(ggplot2)
library(plotly)
library(dplyr)
library(lubridate)
library(randomForest)
library(scales)

real_time_data <- read.csv("merged_data.csv", stringsAsFactors = FALSE)
historical_data <- read.csv("cleaned_historical_data.csv", stringsAsFactors = FALSE)

# Convert dates
real_time_data$last_update <- as.Date(real_time_data$last_update)
historical_data$last_update <- as.Date(historical_data$last_update)

# Define pollutants
pollutants <- c("pm2_5", "pm10", "co", "no2", "so2", "nh3", "ozone")

# UI
ui <- dashboardPage(
  dashboardHeader(title = "Air Pollution Dhruv Wali"),
  dashboardSidebar(
    sidebarMenu(
      menuItem("State-wise Map", tabName = "heatmap", icon = icon("globe")),
      menuItem("City-wise Analysis", tabName = "city_analysis", icon = icon("city")),
      menuItem("Trend Analysis", tabName = "trend_analysis", icon = icon("chart-line")),
      menuItem("Forecasting", tabName = "forecasting", icon = icon("chart-area"))
    )
  ),
  dashboardBody(
    tabItems(
      tabItem(tabName = "heatmap",
              fluidRow(
                box(title = "State-wise Pollution Map", width = 12,
                    selectInput("state_pollutant", "Select Pollutant", choices = pollutants, selected = "pm2_5"),
                    leafletOutput("state_map"))
              )
      ),
      tabItem(tabName = "city_analysis",
              fluidRow(
                box(
                  title = "City-wise Map",
                  width = 6,
                  selectInput("selected_city", "Select City:", choices = unique(real_time_data$city)),
                  selectInput("city_pollutant", "Select Pollutant:",
                              choices = pollutants, selected = "pm2_5"),
                  leafletOutput("city_map", height = 500)
                ),
                box(
                  title = "Pollutant Levels in City",
                  width = 6,
                  plotlyOutput("city_pollutants")
                )
              )
      ),
      tabItem(tabName = "trend_analysis",
              fluidRow(
                box(title = "Weekly Trend Analysis", width = 12,
                    selectInput("trend_city", "Select City", choices = unique(historical_data$city)),
                    selectInput("trend_pollutant", "Select Pollutant", choices = pollutants),
                    plotlyOutput("trend_plot"))
              )
      ),
      tabItem(tabName = "forecasting",
              fluidRow(
                box(title = "Random Forest Forecasting", width = 12,
                    selectInput("forecast_city", "Select City", choices = unique(historical_data$city)),
                    selectInput("forecast_pollutant", "Select Pollutant", choices = pollutants),
                    plotlyOutput("forecast_rf"))
              )
      )
    )
  )
)

# Server
server <- function(input, output, session) {
  # State Map
  output$state_map <- renderLeaflet({
    req(input$state_pollutant)
    
    pollutant_data <- real_time_data[!is.na(real_time_data[[input$state_pollutant]]), ]
    pal <- colorQuantile("Reds", pollutant_data[[input$state_pollutant]], n = 5)
    
    leaflet(pollutant_data) %>%
      addTiles() %>%
      addCircleMarkers(
        lng = pollutant_data$longitude, lat = pollutant_data$latitude,
        radius = scales::rescale(pollutant_data[[input$state_pollutant]], to = c(3, 15)),
        color = ~pal(pollutant_data[[input$state_pollutant]]),
        stroke = TRUE, weight = 2, fillOpacity = 0.7,
        popup = paste0(pollutant_data$city, "<br>", input$state_pollutant, ":", pollutant_data[[input$state_pollutant]])
      ) %>%
      addLegend(pal = pal, values = pollutant_data[[input$state_pollutant]], title = input$state_pollutant)
  })
  
  # City Heat Map
  output$city_map <- renderLeaflet({
    req(input$selected_city, input$city_pollutant)
    
    city_data <- real_time_data[real_time_data$city == input$selected_city, ]
    
    leaflet(city_data) %>%
      addTiles() %>%
      addHeatmap(
        lng = ~longitude, lat = ~latitude,
        intensity = ~get(input$city_pollutant),
        blur = 50, radius = 35, max = NULL
      )
  })
  
  # City Pollutants Bar Plot
  output$city_pollutants <- renderPlotly({
    req(input$selected_city)
    
    city_data <- real_time_data[real_time_data$city == input$selected_city, ]
    pollutant_values <- sapply(pollutants, function(p) mean(city_data[[p]], na.rm = TRUE))
    
    df <- data.frame(Pollutant = pollutants, Value = pollutant_values)
    
    p <- ggplot(df, aes(x = Pollutant, y = Value, fill = Pollutant)) +
      geom_bar(stat = "identity") +
      theme_minimal() +
      labs(title = paste("Pollutant Levels in", input$selected_city), y = "Concentration")
    
    ggplotly(p)
  })
  
  # Trend Analysis
  output$trend_plot <- renderPlotly({
    req(input$trend_city, input$trend_pollutant)
    
    trend_data <- historical_data %>%
      filter(city == input$trend_city & !is.na(.data[[input$trend_pollutant]])) %>%
      group_by(last_update) %>%
      summarize(value = mean(.data[[input$trend_pollutant]], na.rm = TRUE))
    
    gg <- ggplot(trend_data, aes(x = last_update, y = value)) +
      geom_line(color = "blue") +
      geom_point(color = "red") +
      theme_minimal() +
      labs(title = paste("Weekly Trend for", input$trend_pollutant, "in", input$trend_city),
           x = "Date", y = "Concentration")
    
    ggplotly(gg)
  })
  
  output$forecast_rf <- renderPlotly({
    req(input$forecast_city, input$forecast_pollutant)
    
    rf_data <- historical_data %>%
      filter(city == input$forecast_city & !is.na(.data[[input$forecast_pollutant]])) %>%
      arrange(last_update)
    
    if (nrow(rf_data) < 14) {
      showNotification("Not enough data for forecasting", type = "error")
      return(NULL)
    }
    
    n_lags <- 7
    for (i in 1:n_lags) {
      rf_data[[paste0("lag_", i)]] <- dplyr::lag(rf_data[[input$forecast_pollutant]], i)
    }
    
    rf_data <- rf_data %>%
      select(last_update, value = all_of(input$forecast_pollutant), starts_with("lag_")) %>%
      na.omit()
    
    if (nrow(rf_data) < 10 || !all(paste0("lag_", 1:n_lags) %in% names(rf_data))) {
      showNotification("Insufficient lag data", type = "error")
      return(NULL)
    }
    
    predictors <- paste0("lag_", 1:n_lags)
    rf_model <- randomForest(value ~ ., data = rf_data[, c("value", predictors)], ntree = 100)
    
    last_vals <- tail(rf_data$value, n_lags)
    forecast_values <- numeric(7)
    
    for (i in 1:7) {
      input_df <- data.frame(matrix(last_vals, nrow = 1))
      names(input_df) <- predictors
      
      pred <- predict(rf_model, input_df)
      forecast_values[i] <- pred
      
      last_vals <- c(tail(last_vals, n_lags - 1), pred)
    }
    
    future_dates <- seq.Date(from = max(rf_data$last_update) + 1, by = "day", length.out = 7)
    forecast_df <- data.frame(Date = future_dates, Forecast = forecast_values)
    
    ggplotly(
      ggplot(forecast_df, aes(x = Date, y = Forecast)) +
        geom_line(color = "forestgreen") +
        geom_point(color = "darkorange") +
        theme_minimal() +
        labs(title = paste("Random Forest Forecast for", input$forecast_pollutant, "in", input$forecast_city),
             x = "Date", y = "Predicted Concentration")
    )
  })
}

shinyApp(ui, server)
