library(rjson)
library(jsonlite)
library(tidyverse)
library(writexl)
library(httr)
library(RCurl)
library(readr)
library(dplyr)
library(data.table)
library(reshape2)
library(readxl)
rm(list=ls())

#read in list with URLs - weather
directory_weather <- "P:/RAU/PASS/Special Projects/2022/Business Continuity"
directory_local <- "//rb.win.frb.org/F1/Accounts/BSR/D-F/f1axd05/My Documents/R scripts"
weather <- "sixth_district_weather_url.xlsx"


#import files 
weather_urls <- read_excel(paste(directory_weather, weather, sep ='/'))

#subset to first three branches (for testing)
#forecast_urls <- head(forecast_urls, 2)
#weather_urls <- head(weather_urls, 2)

#add index-es
weather_urls_index <- tibble::rowid_to_column(weather_urls, "index")
weather_urls_index$grid <- "1" 


#call pkeep_wide frame to populate with data from loop
#pkeep_wide <- "pkeep_wide.xlsx"
#pkeep_wide_loop <- read_excel(paste(directory_weather, pkeep_wide, sep = '/'))
#pkeep_wide_loop <- pkeep_wide

testit <- function(x)
{
  p1 <- proc.time()
  Sys.sleep(x)
  proc.time() - p1 # The cpu usage should be negligible
}
testit(1.8)


#initialize frame and assign frame in case of error
branch_weather_wide <- as.data.frame(matrix(rep(NA,17), ncol=17))
colnames(branch_weather_wide) <- 
  c('index', 'twd', 'wind_direction', 'tws', 'wind_speed', 'coverage',
    'weather', 'intensity', 'visunit', 'visibility_detail', 'attributes_detail',
    'tha', 'hazard', 'tli', 'lightning', 'thurr', 'probhurrwi')
sapply(branch_weather_wide, class)

# branch_weather_wide <- as.data.frame(read_excel(paste(directory_local, "branch_weather_wide_test.xlsx", 
#                  sep = "/"), col_types=c("integer", "list", "list", "list", 
#                                           "list", "list", "list", "list", 
#                                           "list", "list", "logical", "logical",
#                                           "logical", "list", "list", "logical", 
#                                           "logical")))



#initialize frame for errors
branch_weather_error <- as.data.frame(matrix(rep(NA,17), ncol=17))
colnames(branch_weather_error) <- 
  c('index', 'twd', 'wind_direction', 'tws', 'wind_speed', 'coverage',
    'weather', 'intensity', 'visunit', 'visibility_detail', 'attributes_detail',
    'tha', 'hazard', 'tli', 'lightning', 'thurr', 'probhurrwi')


# alt assignment of branch_weather_error
branch_weather_error <- branch_weather_wide

## FOR TEST ONLY - KEEP 8 top URLs
weather_urls <- head(weather_urls, 30)
weather_urls_index <- tibble::rowid_to_column(weather_urls, "index")
weather_urls_index$grid <- "1"


# progress bar initialization
n_iter <- length(weather_urls_index$index)
pb <- txtProgressBar(min = 0,      # Minimum value of the progress bar
                     max = n_iter, # Maximum value of the progress bar
                     style = 3,    # Progress bar style (also available style = 1 and style = 2)
                     width = 50,   # Progress bar width. Defaults to getOption("width")
                     char = "=")
for (i in 1:n_iter){  
  
  weather_url <- weather_urls_index$weather_url[i]
  
  tryCatch(
  
    {
    
      withCallingHandlers(weather_json <- rjson::fromJSON(file = paste(weather_url),
                        simplify = FALSE), message = function(c) if (inherits(c, "message")){stop("")})
  
      
      weather_json <- rjson::fromJSON(file = paste(weather_url), simplify = FALSE)
                                       
      #str(weather_json)
      df<-as.data.frame(do.call("cbind", weather_json$properties))
      Sys.sleep(2)
  
  
      wd <- as.data.frame(do.call("rbind", df[[19]][[2]]))
      
      if (length(wd[[2]][[1]]) == 0) { 
        
        wd <- as.data.frame(matrix(ncol = 2, nrow = 1))
        wd_names <- c("twd", "wind_direction")
        colnames(wd) <- wd_names
        
      } else { 
        
        wd <- wd[1,]
        wd_names <- c("twd", "wind_direction")
        colnames(wd) <- wd_names
        
      }
      
      
      ws <- as.data.frame(do.call("rbind", df[[20]][[2]]))
      
      
      if (length(ws[[2]][[1]]) == 0) { 
        
        ws <- as.data.frame(matrix(ncol = 2, nrow = 1))
        ws_names <- c("tws", "wind_speed")
        colnames(ws) <- ws_names
        
      } else { 
        
        ws <- ws[1,]
        ws_names <- c("tws", "wind_speed")
        colnames(ws) <- ws_names
        
      }
      
      
      we <- as.data.frame(do.call("rbind", df[[22]][[2]]))
      
      if (length(we[[2]][[1]]) == 0) { 
        
        we <- as.data.frame(matrix(ncol = 5, nrow = 1))
        we_names <- c("coverage", "weather", "intensity", "visibility", "attributes")
        colnames(we) <- we_names
        
        
      } else { 
        
        we <- as.data.frame(do.call("rbind", we[[2]][[1]]))
        we_names <- c("coverage", "weather", "intensity", "visibility", "attributes")
        colnames(we) <- we_names
      
      }
        
      
      we_vis <- as.data.frame(do.call("rbind", we[[4]]))
      we_vis_names <- c("visunit", "visibility_detail")
      colnames(we_vis) <- we_vis_names
      weather <- cbind(we, we_vis)
      rownames(weather) <- 1:nrow(weather)
      
    
      we_attr <- as.data.frame(matrix(ncol = 1, nrow = 1))
      we_attr_names <- c("attributes_detail")
      colnames(we_attr) <- we_attr_names
      weather <- cbind(weather, we_attr)
      rownames(weather) <- 1:nrow(weather) 
      
      weather <- subset(weather, select = c(coverage, weather, intensity, visunit, visibility_detail, attributes_detail))
      
      
      
      ha <- as.data.frame(do.call("rbind", df[[23]][[2]]))
      
    
      if (length(ha) == 0) { 
        
        ha <- as.data.frame(matrix(ncol = 2, nrow = 1))
        ha_names <- c("tha", "hazard")
        colnames(ha) <- ha_names
        
      } else { 
        
        ha <- as.data.frame(do.call("rbind", ha[[2]][[1]]))
        ha_names <- c("tha", "hazard")
        colnames(ha) <- ha_names
        
      }
      
      
      li <- as.data.frame(do.call("rbind", df[[35]][[2]]))
      
      
      if (length(li[[2]][[1]]) == 0) { 
        
        li <- as.data.frame(matrix(ncol = 2, nrow = 1))
        li_names <- c("tli", "lightning")
        colnames(li) <- hd_names
        
      } else { 
        
        li <- li[1,]
        li_names <- c("tli", "lightning")
        colnames(li) <- li_names
        
      }
      
      
      probhurrwi <- as.data.frame(do.call("rbind", df[[50]][[2]]))
      
      if (length(probhurrwi) == 0) { 
        
        probhurrwi <- as.data.frame(matrix(ncol = 2, nrow = 1))
        probhurrwi_names <- c("thurr", "probhurrwi")
        colnames(probhurrwi) <- probhurrwi_names
        
      } else { 
        
        we <- as.data.frame(do.call("rbind", we[[2]][[1]]))
        probhurrwi_names <- c("thurr", "probhurrwi")
        colnames(probhurrwi) <- probhurrwi_names
        
      }
      
      
      wind_direction <- tibble::rowid_to_column(wd, "index")
      wind_speed <- tibble::rowid_to_column(ws, "index")
      weather_detail <- tibble::rowid_to_column(weather, "index")
      hazard <- tibble::rowid_to_column(ha, "index")
      light <- tibble::rowid_to_column(li, "index")
      probhurrwinds <- tibble::rowid_to_column(probhurrwi, "index")
      
      branch_weather <- list(wind_direction, wind_speed, weather_detail, hazard, light, probhurrwinds) %>% reduce(left_join, by = "index")
      
      branch_weather_wide[i,] <- branch_weather 
      
      },
      
      error=function(e) branch_weather_wide[i,] <- branch_weather_error)
  
  setTxtProgressBar(pb, i) # progress bar
}

close(pb) # closing progress bar

# str(branch_weather)
# str(unlist(branch_weather))
# rm(branch_weather_wide)
