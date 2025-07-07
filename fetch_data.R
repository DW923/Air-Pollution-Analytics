library(httr)
library(jsonlite)
library(dplyr)
library(lubridate)

# API Keys
openweather_api_key <- "YOUR-API-KEY"
cpcb_api_key <- "YOUR-API-KEY"

# CPCB API URL
cpcb_url <- "https://api.data.gov.in/resource/YOUR-CODE"

# Tamil Nadu Cities
locations <- data.frame(
  city = c("Chennai", "Coimbatore", "Madurai", "Ooty", "Perundurai", "Ramanathapuram",
           "Salem", "Thoothukudi", "Cuddalore", "Gummidipoondi", "Kanchipuram",
           "Tirunelveli", "Tirupur", "Karur", "Vellore", "Dindigul", "Tiruchirappalli",
           "Ariyalur"),
  lat = c(13.0827, 11.0168, 9.9252, 11.4102, 11.2740, 9.3716, 11.6643, 8.8050,
          11.7447, 13.4074, 12.8352, 8.7139, 11.1085, 10.9601, 12.9165, 10.3662, 10.7905,
          11.1400),
  lon = c(80.2707, 76.9558, 78.1198, 76.6950, 77.5820, 78.8323, 78.1460, 78.1348,
          79.7680, 80.1086, 79.7036, 77.7567, 77.3411, 78.0766, 79.1325, 77.9710, 78.7047,
          79.0780)
)


# Function to fetch OpenWeather real-time air pollution data
fetch_openweather_realtime <- function(lat, lon, city_name, num_records = 1, interval_sec = 5) {
  realtime_data <- list()
  
  for (i in 1:num_records) {
    openweather_url <- paste0(
      "http://api.openweathermap.org/data/2.5/air_pollution?",
      "lat=", lat, "&lon=", lon, "&appid=", openweather_api_key
    )
    
    response <- GET(openweather_url)
    
    if (status_code(response) != 200) {
      message("Failed to fetch real-time OpenWeather data for: ", city_name)
      return(NULL)
    }
    
    data <- fromJSON(content(response, "text"), flatten = TRUE)
    
    if (!"list" %in% names(data) || length(data$list) == 0) {
      message("No real-time data available for: ", city_name)
      return(NULL)
    }
    
    pollution_data <- data$list[1, ]
    
    df <- data.frame(
      city = city_name,
      latitude = lat,
      longitude = lon,
      aqi = pollution_data$main.aqi,
      pm2_5 = pollution_data$components.pm2_5,
      pm10 = pollution_data$components.pm10,
      co = pollution_data$components.co,
      no2 = pollution_data$components.no2,
      so2 = pollution_data$components.so2,
      last_update = with_tz(Sys.time(), "Asia/Kolkata")
    )
    
    realtime_data[[i]] <- df
    
    message("Real-time record ", i, " fetched for: ", city_name)
    
    Sys.sleep(interval_sec)  # Wait before next API call
  }
  
  return(do.call(rbind, realtime_data))
}

# Function to fetch OpenWeather historical air pollution data for the past 7 days
fetch_openweather_historical <- function(lat, lon, city_name) {
  historical_data <- list()
  
  for (days in 1:7) {
    start_time <- as.numeric(as.POSIXct(Sys.Date() - days, tz = "UTC"))
    end_time <- start_time + 86400  # +1 day in seconds
    
    openweather_hist_url <- paste0(
      "http://api.openweathermap.org/data/2.5/air_pollution/history?",
      "lat=", lat, "&lon=", lon, "&start=", start_time, "&end=", end_time, "&appid=", openweather_api_key
    )
    
    response <- GET(openweather_hist_url)
    
    if (status_code(response) != 200) {
      message("Failed to fetch OpenWeather historical data for: ", city_name, " on Day ", days)
      next
    }
    
    data <- fromJSON(content(response, "text"), flatten = TRUE)
    
    if (!"list" %in% names(data) || length(data$list) == 0) {
      message("No historical data available for: ", city_name, " on Day ", days)
      next
    }
    
    pollution_data <- data$list
    
    df <- data.frame(
      city = city_name,
      latitude = lat,
      longitude = lon,
      aqi = pollution_data$main.aqi,
      pm2_5 = pollution_data$components.pm2_5,
      pm10 = pollution_data$components.pm10,
      co = pollution_data$components.co,
      no2 = pollution_data$components.no2,
      so2 = pollution_data$components.so2,
      last_update = with_tz(as.POSIXct(pollution_data$dt, origin = "1970-01-01", tz = "UTC"), "Asia/Kolkata")
    )
    
    historical_data <- append(historical_data, list(df))
    
    message("Historical data fetched for: ", city_name, " on Day ", days)
  }
  
  if (length(historical_data) == 0) {
    message("No historical data available for: ", city_name)
    return(NULL)
  }
  
  return(bind_rows(historical_data))
}

# Function to fetch CPCB data
fetch_cpcb_data <- function() {
  response <- GET(cpcb_url, query = list(
    "api-key" = cpcb_api_key,
    format = "json",
    limit = 5000
  ))
  
  if (status_code(response) != 200) {
    message("Failed to fetch CPCB data")
    return(NULL)
  }
  
  cpcb_raw <- content(response, "text")
  cpcb_data <- fromJSON(cpcb_raw, flatten = TRUE)
  
  if (!"records" %in% names(cpcb_data) || length(cpcb_data$records) == 0) {
    message("No CPCB data found")
    return(NULL)
  }
  
  cpcb_records <- as.data.frame(cpcb_data$records)
  
  # Filter for Tamil Nadu
  cpcb_tn_data <- cpcb_records %>% filter(state == "TamilNadu")
  
  message("CPCB data fetched successfully (", nrow(cpcb_tn_data), " rows)")
  return(cpcb_tn_data)
}

# Fetch real-time data (OpenWeather)
realtime_data <- do.call(rbind, lapply(1:nrow(locations), function(i) {
  fetch_openweather_realtime(locations$lat[i], locations$lon[i], locations$city[i], num_records = 1)
}))

# Fetch historical data (OpenWeather)
historical_data <- lapply(1:nrow(locations), function(i) {
  fetch_openweather_historical(locations$lat[i], locations$lon[i], locations$city[i])
}) %>% bind_rows()

# Fetch CPCB data
cpcb_data <- fetch_cpcb_data()

write.csv(realtime_data, "openweather_realtime.csv", row.names = FALSE)
write.csv(historical_data, "openweather_historical.csv", row.names = FALSE)
write.csv(cpcb_data, "cpcb_data.csv", row.names = FALSE)

message("OpenWeather real-time, historical, and CPCB data saved.")
