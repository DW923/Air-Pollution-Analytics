library(dplyr)
library(lubridate)
library(tidyr)

# Load datasets
tn_cpcb_data <- read.csv("cpcb_data.csv", stringsAsFactors = FALSE)
tn_open_real <- read.csv("openweather_realtime.csv", stringsAsFactors = FALSE)
tn_open_hist <- read.csv("openweather_historical.csv", stringsAsFactors = FALSE)

#Matching date formats across different datasets
tn_cpcb_data$last_update <- dmy_hms(tn_cpcb_data$last_update, tz = "Asia/Kolkata")
tn_open_real$last_update <- ymd_hms(tn_open_real$last_update, tz = "Asia/Kolkata")
tn_open_hist$last_update <- ymd_hms(tn_open_hist$last_update, tz = "Asia/Kolkata")

tn_cpcb_data$last_update <- as.Date(tn_cpcb_data$last_update)
tn_open_real$last_update <- as.Date(tn_open_real$last_update)
tn_open_hist$last_update <- as.Date(tn_open_hist$last_update)

#CPCB Data pre-processing
# Compute average pollutant value
tn_cpcb_data$avg_value <- rowMeans(select(tn_cpcb_data, min_value, max_value), na.rm = TRUE)

tn_cpcb_wide <- tn_cpcb_data %>%
  select(city, latitude, longitude, pollutant_id, avg_value, last_update) %>%
  spread(key = pollutant_id, value = avg_value) #in wide format

colnames(tn_cpcb_wide) <- tolower(colnames(tn_cpcb_wide))
colnames(tn_cpcb_wide) <- gsub(" ", "_", colnames(tn_cpcb_wide))

pollutant_cols <- c("pm2_5", "pm10", "co", "no2", "so2", "nh3", "ozone")

# Ensure all pollutant columns exist in CPCB data
for (col in pollutant_cols) {
  if (!(col %in% colnames(tn_cpcb_wide))) {
    tn_cpcb_wide[[col]] <- NA
  }
}

#Merging CPCB data with real-time OpenWeather data
merged_data <- full_join(tn_open_real, tn_cpcb_wide, by = c("city", "latitude", "longitude"))

if ("last_update.x" %in% colnames(merged_data) & "last_update.y" %in% colnames(merged_data)) {
  merged_data <- merged_data %>%
    mutate(last_update = coalesce(last_update.x, last_update.y)) %>%
    select(-last_update.x, -last_update.y)
}

pollutant_cols_original <- c("pm2_5", "pm10", "co", "no2", "so2", "nh3", "ozone")

for (col in pollutant_cols_original) {
  col_x <- paste0(col, ".x")
  col_y <- paste0(col, ".y")
  
  if (col_x %in% colnames(merged_data) & col_y %in% colnames(merged_data)) {
    merged_data <- merged_data %>%
      mutate(!!col := coalesce(.data[[col_x]], .data[[col_y]])) %>%
      select(-all_of(c(col_x, col_y)))
  }
}

#Handling missing pollutant values
for (col in pollutant_cols_original) {
  if (col %in% colnames(merged_data)) {
    merged_data <- merged_data %>%
      group_by(city) %>%
      mutate(!!col := ifelse(is.na(.data[[col]]), first(na.omit(.data[[col]])), .data[[col]])) %>%
      ungroup()
  }
}

merged_data <- merged_data %>%
  select(city, latitude, longitude, aqi, pm2_5, pm10, co, no2, so2, nh3, ozone, last_update)

merged_data <- merged_data %>%
  mutate(across(where(is.numeric), ~ ifelse(is.na(.), median(., na.rm = TRUE), .)))

if ("converted_city" %in% colnames(tn_open_hist)) {
  tn_open_hist <- tn_open_hist %>% select(-converted_city)
}

#Historical OpenWeather Data pre-processing
# Replace NA values with column-wise median
tn_open_hist <- tn_open_hist %>%
  mutate(across(where(is.numeric), ~ ifelse(is.na(.), median(., na.rm = TRUE), .)))

write.csv(merged_data, "merged_data.csv", row.names = FALSE)
write.csv(tn_open_hist, "cleaned_historical_data.csv", row.names = FALSE)

print("Data pre-processing and merging completed.")
