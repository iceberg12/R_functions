rm(list=ls())

# for (dayspec in 22:22){
#   
#   for (hourspec in seq(2, 23, by=3)){
#     setwd("~/PdM/IM_filament")
#     a <- system('kinit email -kt /home/hdfswrite/.keytab/hdfswrite.keytab', intern = T)
    tryCatch({
      args <- commandArgs(trailingOnly=T)
      trend.window <- as.numeric(args[1])
      sys.time <- strptime(args[2], "%Y-%m-%d %H:%M:%S GMT")
      if (nchar(args[2]) < 23){
        print("Given time is not sufficient")
      }
      header.t2 <- args[3]
      file.t2 <- args[4]
      header.t3 <- args[5]
      file.t3 <- args[6]
      success.t <- args[7]
      source.path <- args[8]
      local.path <- args[9]
      pm.path <- args[10]
      
      Sys.setenv(HADOOP_HOME="/usr/hdp/current/hadoop-client")
      Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
      Sys.setenv(HADOOP_CONF_DIR="/etc/hadoop/conf")
      Sys.setenv(HIVE_HOME="/usr/hdp/current/hive-client")
      Sys.setenv(HADOOP_STREAMING="/usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming-2.6.0.2.2.0.0-2041.jar")
      source(paste0(source.path, "/", "functions.R")) #? Need to change back to correct folder
      library(rhdfs)
      hdfs.init()
      
      time.name <- format(sys.time, "%Y-%m-%d %H:%M:%S GMT")
      time.name <- substr(time.name, 1, nchar(time.name)-10)
      time.name <- gsub("-", "", time.name)
      time.name <- gsub(" ", "T", time.name)
      
      ######################################
      # Need to cut off all points before last PM
      # Read PM data within last 5 days
      print("Reading PM data from HDFS ...")
      
      df_pm <- local({
#         pm.db <- "eng_pub_equiptrkg"
#         if (grepl("10w", pm.path, ignore.case=T)==T){
#           pm.table <- "equip_history_f10w"
#           pm.table.des <- "pdm.im_equip_hist_f10w"
#         } else {
#           pm.table <- "equip_history_f10n"
#           pm.table.des <- "pdm.im_equip_hist_f10n"
#         }
#         time.name.1 <- format(sys.time - as.difftime(trend.window, units="days"), "%Y-%m-%d %H:%M:%S GMT")
#         time.name.1 <- substr(time.name.1, 1, nchar(time.name.1)-4)
#         time.name.2 <- format(sys.time, "%Y-%m-%d %H:%M:%S GMT")
#         time.name.2 <- substr(time.name.2, 1, nchar(time.name.2)-4)
#         
#         hive.query.1 <- paste0("beeline -u \"jdbc:hive2://website.com:10010/", pm.db, ";principal=hive/server\" -e ",
#                                "\"CREATE TABLE IF NOT EXISTS ", pm.table.des,
#                                " (equip_id STRING, mfg_area_id STRING, semi_state_id STRING, equip_state_id STRING,
#                                event_code_id STRING, equip_state_in_datetime TIMESTAMP, equip_state_out_datetime TIMESTAMP, 
#                                event_hist_mod_dt TIMESTAMP, event_note_text STRING, hive_load_datetime_gmt TIMESTAMP, equip_state_out_year_month STRING)
#                                ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
#                                \"")
#         hive.query.2 <- paste0("beeline -u \"jdbc:hive2://website.com:10010/", pm.db, ";principal=hive/server\" -e ",
#                                "\"INSERT INTO TABLE pdm.im_equip_hist_f10w 
#                                SELECT equip_id, mfg_area_id, semi_state_id, equip_state_id, event_code_id, equip_state_in_datetime, equip_state_out_datetime, event_hist_mod_dt, event_note_text, hive_load_datetime_gmt, equip_state_out_year_month FROM ", pm.table,
#                                " WHERE equip_id like 'IMMC%' AND equip_state_in_datetime > cast('", time.name.1, "' as timestamp) AND equip_state_in_datetime <= cast('", time.name.2, "' as timestamp)
#                                \"")
#         system(hive.query.1)
#         system(hive.query.2)
        
        file.in.dir <- hdfs.ls(pm.path)$file
        valid.file <- file.in.dir
        col.name <- c("equip_id", "mfg_area_id", "semi_state_id", "equip_state_id", "event_code_id", "equip_state_in_datetime",
                      "equip_state_out_datetime", "event_code_hist_mod_dt", "event_note_text", "updated_datetime")      
        df.L1 <- lapply(valid.file, function(file.path){      
          df <- ReadFile(file.path, ext="hive", col.names=col.name)        
        })
        df <- rbindlist(df.L1, use.names = T)
        
        df <- df[which(df$equip_state_id == "IN_PM"), .(equip_id, equip_state_id, equip_state_in_datetime)]

#        hdfs.delete(pm.path)
        df$equip_state_in_datetime <- as.POSIXct(df$equip_state_in_datetime, format="%Y-%m-%d %H:%M:%S")
        
        setkey(df, equip_state_in_datetime)
        df
      })
      
      # =====================================================
      # Reading data
      print("Reading Predict Tables from HDFS...")
      
      no.day.his <- trend.window
      ctx.name <- c("tool_name", "recipe_name", "traveler_step", "part_type", "start_time")
      # file.path <- hdfs.ls(header.t2)$file
      file.path <- paste0(header.t2, "/IM_col_header2.csv")
      col.name <- ReadFile(file.path, col.name="name", ext="csv")[, name]
      
      file.path <- file.t2
      file.in.dir <- hdfs.ls(file.path)$file
      # sort file name so that latest file is at the end. IMPORTANT SO NAME FILE CORRECTLY
      valid.file <- sort(file.in.dir[grep(".csv",file.in.dir)])
      pos <- gregexpr(pattern = "/", valid.file)
      pos <- sapply(pos, function(x) tail(x, 1))
      time <- substr(valid.file, pos + 1, pos + 11)
      time <- as.POSIXct(time, format="%Y%m%dT%H")
      day <- difftime(sys.time, time, units="days")
      i <- (day >= 0) & (day < trend.window + 0.5)  # looking at maximum trend.window days to calculate trend
      valid.file <- valid.file[i]
      
      data.list <- lapply(valid.file, function(file.path) {
        print(file.path)
        df <- ReadFile(file.path, col.name=col.name, ext="csv")
        if (ncol(df) != length(col.name)) {return (NULL)}
        df <- df[, .(tool_name, traveler_step, recipe_name, start_time, predicted_output, performance_idx, remaining_life)]
        # df <- df[, list(start_time=last(start_time), remaining_life=mean(remaining_life)), by=tool_name]
      })
      data <- rbindlist(data.list, use.names = T)
      if (nrow(data) == 0){
        print("Expected Error: No data from prediction tables. Not enough point to forecast.")
      }
      data$start_time <- as.POSIXct(data$start_time, format="%Y-%m-%d %H:%M:%S")
      setkey(data, start_time)
      # ================================================
      print("Forecasting the next maintenance...")
      
      # only consider the tools that appear in the latest file (possibly the last 3 hours)
      i <- length(data.list)
      par.new <- unique(paste(data.list[[i]]$tool_name))
      par <- paste(data$tool_name)
      
      i = which(par %in% par.new)
      if (length(i) > 0) {
        data <- data[i,]
      } else {
        stop("Warning: No model exists for new incoming data.")
      }
      par <- par[i]
      df.list <- split(data, par)
      
      min.nrow <- 30
      a <- list()
      max.run <- 50000
      
      a <- mapply(function(df, name){
        
        # Remove all data before recent PM for this partition
        nam <- str_sub(df$tool_name[1], 1, 8) 
        df_pm1 <- df_pm[grepl(nam, df_pm$equip_id), ]
        
        df <- arrange(df, start_time)
        t <- round(df$start_time,"hours")
        
        # print("Error: Remember to on PM")
        if (nrow(df_pm1) > 0){
          pm.time <- tail(df_pm1$equip_state_in_datetime, 1)
          t1 <- difftime(t, pm.time, units="hours")
          df <- df[t1 > 0, ]
        }
         
        # If the last current value are above 60, just give full life instead of calculating
        i <- nrow(df)
        if (df$predicted_output[i] >= 60){
          rs <- data.frame(tool_name=df$tool_name[1], start_time=max(df$start_time), run_to_maintenance=max.run, day_to_maintenance=28)
          return rs
        }
        
        # Calculation, but have to remove points where current > 60
        df <- df[predicted_output <= 60,]
        if (nrow(df)<min.nrow) {
          rs <- data.frame(tool_name=df$tool_name[1], start_time=max(df$start_time), run_to_maintenance=NULL, day_to_maintenance=NULL)
          return rs
        }
        signal <- rollmedian(zoo(df$remaining_life, df$start_time), k=5)
        signal <- coredata(signal)
        fc <- holt(signal, h=max.run)
        fc <- as.numeric(fc$mean)
        i <- which((fc >= 0 & fc < 0.05) | fc < 0)
        if (length(i)==0) {
          rtm <- max.run
        } else {
          rtm <- i[1]
        }
        dtm <- FocastLifeEnd(df$start_time[-c(1:4)], data.frame(signal), 0, nWeeks=4/7, type1='day', lifeBeg=30)
        
        # start_time is the latest start_time for each 3 hours.
        if (!is.na(dtm)){
          rs <- data.frame(tool_name=df$tool_name[1], start_time=max(df$start_time), run_to_maintenance=rtm, day_to_maintenance=dtm)
        } else {
          rs <- data.frame(tool_name=df$tool_name[1], start_time=max(df$start_time), run_to_maintenance=NULL, day_to_maintenance=NULL)
        }
      }, df.list, names(df.list), SIMPLIFY=F)

      if (!all(sapply(a, is.null))) {
        forecast.table <- rbindlist(a, use.names = T)
      } else {
        forecast.table <- data.frame()
      }

      # ================================================
      # Write data to HDFS
      print("Writing forecasting data to HDFS...")
      
      stopifnot(nrow(forecast.table) > 0)
      
      if (nrow(forecast.table) > 0) {
        print("Write table to HDFS ...")
        file.name <- sys.time
        file.name <- format(file.name, "%Y-%m-%d %H:%M:%S GMT")
        file.name <- substr(file.name, 1, nchar(file.name)-10)
        file.name <- gsub("-", "", file.name)
        file.name <- gsub(" ", "T", file.name)
        file.name
        path <- paste0(local.path, "/test/forecast")
        file.path <- paste0(path, "/", file.name,".csv")
        header.path <- paste0(path, "/col_header/IM_col_header3.csv")
        success.path <- paste0(path, "/forecast_success.csv")
        
        dir.create(paste0(path,"/col_header"), recursive=T)
        
        write.table(data.frame(col_header=names(forecast.table)),
                    header.path, quote=F, row.names=F, sep=",", col.names=FALSE)
        write.table(format(forecast.table, digits=12), 
                    file.path, quote=F, row.names=F, sep=",", col.names=FALSE)
        write.table(data.frame(a="Sury Chenbin Minh cheers :))"),
                    success.path, quote=F, row.names=F, sep=",", col.names=FALSE)
        
        # Upload files through rhdfs
        hdfs.put(header.path, header.t3)
        hdfs.put(file.path, file.t3)
        hdfs.put(success.path, success.t)
        
        # test1 <- read.csv(header.path, col.names="name", header=F)[, "name"]
        # test2 <- read.csv(file.path, col.names=test1, header=F)

        # notify by email
        for (i in 1:nrow(forecast.table)){
            cat('Need to implement email notification.')
        }
      }
    }, error = function(e) {
      print(e)
      file.name <- paste0(time.name, "_forecast_fail")
      path <- paste0(local.path, "/test/forecast")
      dir.create(path, recursive=T)
      success.path <- paste0(path, "/", file.name, ".csv")
      
      write.table(data.frame(a=c(trend.window, sys.time, header.t2, file.t2,  header.t3, file.t3, success.t, source.path, local.path, as.character(e)),
                             spec=c("trend.window", "sys.time", "header.t2", "file.t2", "header.t3", "file.t3", "success.t", "source.path", "local.path", "error")),
                  success.path, quote=F,
                  sep=",", col.names=FALSE)
      hdfs.put(success.path, success.t) 
    })
#   }
# }
