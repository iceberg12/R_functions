# pkgs = names(sessionInfo()$otherPkgs)
# pkgs = paste('package:', pkgs, sep = "")
# lapply(pkgs, detach, character.only = TRUE, unload = TRUE)

for (dayspec in 15:20){
  for (hourspec in seq(2, 23, by=3)){
    rm(list=ls())
    
#     setwd("~/PdM/IM_filament")
#     a <- system('kinit hdfswrite@NA.MICRON.COM -kt /home/hdfswrite/.keytab/hdfswrite.keytab', intern = T)
    tryCatch({    
      no.hour.predict <- 3  
      sys.time <- strptime(paste0("2016-01-", dayspec, " ", hourspec, ":00:00 GMT"), "%Y-%m-%d %H:%M:%S GMT")
      file.fd <- "/eng/mti/singapore/fab_7/predictive_maintenance/idl/IMMC__ALL/IMMC__ALL_Model_Input"
      header.t2 <- "/eng/mti/singapore/fab_7/predictive_maintenance/pub_modeloutput/IMMC__ALL/pdm_model_output_col_header"
      file.t2 <- "/eng/mti/singapore/fab_7/predictive_maintenance/pub_modeloutput/IMMC__ALL/pdm_model_output_prediction"
      success.t <- "/eng/mti/singapore/fab_7/predictive_maintenance/pub_modeloutput/IMMC__ALL/successfactors"
      source.path <- "."
      #     source.path <- "/home/etldata/POC_cases/PROD_Version_SVN/FD_trace_point/scripts/model_scripts/IM_filament"
      local.path <- "/home/hdfsf10w/PdM/IM_filament"
      pm.path <- "/apps/hive/warehouse/user_mikenguyen.db/im_equip_hist_f10w"
      
#       args <- commandArgs(trailingOnly=T)
#       no.hour.predict <- as.numeric(args[1])
#       sys.time <- strptime(args[2], "%Y-%m-%d %H:%M:%S GMT")
#       if (nchar(args[2]) < 23){
#         stop("Given time is not sufficient")
#       }
#       file.fd <- args[3]
#       header.t2 <- args[4]
#       file.t2 <- args[5]
#       success.t <- args[6]
#       source.path <- args[7]
#       local.path <- args[8]
#       pm.path <- args[9]

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
      # =====================================================
      # Reading data
      
      # Read FD data
      print("Reading FD data from HDFS ...")
      no.hour.history <- 4*24
      
      df_FD <- local({
        
        file.in.dir <- hdfs.ls(file.fd)$file  # Read all file from fd path
        files <- file.in.dir[grep("T[0-9]{2}.bz2$", file.in.dir)]
        
        valid.file <- files
        if (length(valid.file) == 0) print("Error: No FD file to read!")
        pos <- gregexpr(pattern = "/", valid.file)
        pos <- sapply(pos, function(x) tail(x, 1))
        file.name <- substr(valid.file, pos + 1, pos + 11)
        time <- as.POSIXct(file.name, format="%Y%m%dT%H")
        hour <- difftime(sys.time, time, units="hours")
        i <- (hour >= 0) & (hour < no.hour.history+0.5)
        last_hour <- (hour >= 0) & (hour < no.hour.predict+0.5)
        if (sum(last_hour) == 0){
          warning("Warning: No new data within the latest 3 hour.")
        }
        valid.file <- valid.file[i]
        
        # Remove old files not in valid.file
        des <- paste0(local.path, "/", "download")
        file.name <- paste0(c(paste0(file.name[i], '.bz2'), paste0(file.name[i], '_header.bz2')))
        local.file.name <- list.files(des)
        file.del <- setdiff(local.file.name, file.name)
        if (length(file.del) > 0) {
          do.call(file.remove, list(paste0(des, '/', file.del)))
        }
        
        # Only read interested FD signals
        selCol=c("traveler_step", "lot_id", "wafer_id", "part_type", "recipe_name", "start_time", "tool_name",
                 "1_1_FloodgunFilamentCurrent_average", "1_1_PFGTimeHot_average")
        data.list <- lapply(valid.file, function(file.path) {
          print(file.path)
          
          # handle header and corresponding file
          header.path <- str_replace(file.path, ".bz2", "_header.bz2")
          system.time(df <- ReadFile2(local.path, header.path, file.path, ext="tsv", selCol))
          
          # convert lot_id to string with sufficient digits
          i <- which(sapply(df$lot_id, is.numeric))
          df$lot_id[i] <- sprintf("%011.3f", df$lot_id[i])
          df
        })
        system.time(df_FD <- rbindlist(data.list, use.names=T))
        setkey(df_FD, tool_name, recipe_name, start_time)
        
        i <- length(data.list)    
        par.new <- unique(c(paste(data.list[[i]]$tool_name, data.list[[i]]$recipe_name, sep=":"),
                            paste(data.list[[i-1]]$tool_name, data.list[[i-1]]$recipe_name, sep=":"))) 
        par <- paste(df_FD$tool_name, df_FD$recipe_name, sep=":")
        
        i = which(par %in% par.new)
        if (length(i) == 0){
          stop("Error: Error in joining data.")
        }
        df_FD <- df_FD[i,]
        df_FD <- df_FD[!(df_FD$recipe_name == '\\N'),] # remove unknown recipe
        df_FD
      })
      
      # Preprocessing
      print("Preprocessing data to include certain tools and interested signals ...")
      df_FD <- local({
        df_FD$start_time <- as.POSIXct(df_FD$start_time, format="%Y-%m-%d %H:%M:%S")  # change time format
        
        # add "x" to sensor column names
        i <- !is.na(as.numeric(str_sub(names(df_FD), 1, 1)))
        if (any(i)) setnames(df_FD, names(df_FD)[i], str_c("x", names(df_FD)[i]))
        
        # change sensor format from character to numeric
        sensor <- "x1_1_FloodgunFilamentCurrent_average"
        df_FD <- as.data.frame(df_FD)
        df_FD[,sensor] <- as.numeric(df_FD[,sensor])
        df_FD <- unique(df_FD, by=c("tool_name", "recipe_name", "traveler_step", "start_time")) # remove lots of duplicated rows (30%)
        
        # Create column "y" to collect correct the signal to monitor
        df_FD$y <- df_FD$x1_1_FloodgunFilamentCurrent_average  # Get the signal depending on tool and traveler step
        df_FD$pfgTime <- df_FD$x1_1_PFGTimeHot_average
        df_FD$signal <- rep("1_1_FloodgunFilamentCurrent_average", nrow(df_FD))
        
        # Remove rows with NA start_time or y
        df_FD <- data.table(df_FD)
        df_FD <- df_FD[!is.na(df_FD$start_time) & !is.na(df_FD$y),]
        setkey(df_FD, start_time)
        df_FD
      })
      
      
      ######################################
      # Need to cut off all points before last PM
      # Read PM data within last 5 days
      print("Reading PM data from HDFS ...")
      
      df_pm <- local({

        if (grepl("10w", pm.path, ignore.case=T)==T){
          pm.table <- "eng_pub_equiptrkg.equip_history_f10w"
          pm.table.des <- "user_mikenguyen.im_equip_hist_f10w"
        } else {
          pm.table <- "eng_pub_equiptrkg.equip_history_f10n"
          pm.table.des <- "user_mikenguyen.im_equip_hist_f10n"
        }
        time.name.1 <- format(sys.time - as.difftime(5, units="days"), "%Y-%m-%d %H:%M:%S GMT")
        time.name.1 <- substr(time.name.1, 1, nchar(time.name.1)-4)
        time.name.2 <- format(sys.time, "%Y-%m-%d %H:%M:%S GMT")
        time.name.2 <- substr(time.name.2, 1, nchar(time.name.2)-4)
        hive.query.1 <- paste0("beeline -u \"jdbc:hive2://tshdpprod01-hive.wlsg.micron.com:10010/default;principal=hive/tslhdppname2.wlsg.micron.com@HADOOP.MICRON.COM?tez.queue.name=eng_f10w-04\" -e ",
                               "\"CREATE TABLE IF NOT EXISTS ", pm.table.des,
                               " (equip_id STRING, mfg_area_id STRING, semi_state_id STRING, equip_state_id STRING,
                               event_code_id STRING, equip_state_in_datetime TIMESTAMP, equip_state_out_datetime TIMESTAMP, 
                               event_hist_mod_dt TIMESTAMP, event_note_text STRING, hive_load_datetime_gmt TIMESTAMP, equip_state_out_year_month STRING)
                               ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
                               \"")
        hive.query.2 <- paste0("beeline -u \"jdbc:hive2://tshdpprod01-hive.wlsg.micron.com:10010/default;principal=hive/tslhdppname2.wlsg.micron.com@HADOOP.MICRON.COM?tez.queue.name=eng_f10w-03\" -e ",
                               "\"INSERT OVERWRITE TABLE ", pm.table.des, 
                               " SELECT equip_id, mfg_area_id, semi_state_id, equip_state_id, event_code_id, equip_state_in_datetime, equip_state_out_datetime, event_hist_mod_dt, event_note_text, hive_load_datetime_gmt, equip_state_out_year_month FROM ", pm.table,
                               " WHERE equip_id like 'IMMC%' AND equip_state_in_datetime > cast('", time.name.1, "' as timestamp) AND equip_state_in_datetime <= cast('", time.name.2, "' as timestamp)
                               \"")
        system(hive.query.1)
        system(hive.query.2)
        
        a1 <- hdfs.ls(pm.path)
        valid.file <- a1[a1$size > 0, ]$file
        
        col.name <- c("equip_id", "mfg_area_id", "semi_state_id", "equip_state_id", "event_code_id", "equip_state_in_datetime",
                      "equip_state_out_datetime", "event_hist_mod_dt", "event_note_text", "hive_load_datetime_gmt", "equip_state_out_year_month")      
        df.L1 <- lapply(valid.file, function(file.path){      
          df <- ReadFile(file.path, ext="hive", col.names=col.name)        
        })
        df <- rbindlist(df.L1, use.names = T)
        hdfs.delete(pm.path)
      
        df <- df[which(df$event_code_id %in% c("IN_PM", "PM_12_WEEK", "PM_24_WEEK", "PM_48_WEEK", "PM_FLOODGUN", "PM_MONTHLY", "PM_48_WEEK_P1",
                                               "PM_48_WEEK_P2", "PM_48_WEEK_P3", "PM_48_WEEK_P4", "PM_BIWEEKLY", "PM_FLOODGUN", "PM_12_WEEK",
                                               "PM_24_WEEK", "PM_48_WEEK", "PM_FLOODGUN", "PM_MONTHLY", "PM_48_WEEK_P1", "PM_48_WEEK_P2",
                                               "PM_48_WEEK_P3", "PM_48_WEEK_P4", "PM_MONTHLY")
                       | ((df$semi_state_id=="UNSCHEDULED_DOWNTIME") & (df$equip_state_id %in% c('WAIT_REPAIR', 'IN_REPAIR'))
                          )),]
        df$equip_state_in_datetime <- as.POSIXct(df$equip_state_in_datetime, format="%Y-%m-%d %H:%M:%S")
        
        # only get PM/CM time that before sys.time (in case wrong PM files read)
        t <- round(df$equip_state_in_datetime, "hours")
        t1 <- difftime(t, sys.time, units="hours")
        df <- df[t1 <= 0, ]
        setkey(df, equip_state_in_datetime)

        df
      })
      
      ######################################
      # Create Prediction table
      print("Creating the Prediction table ...")
      
      predict.table <- local({
        ctx.name <- c("tool_name", "recipe_name", "traveler_step", "part_type", "lot_id", "wafer_id", "start_time", "signal")
        
        #!!! REMOVE all points with current < 20
        df_FD <- df_FD[df_FD$y > 20,]
        
        predict.table <- as.data.frame(df_FD[, ctx.name, with=F]) 
        predict.table$predicted_output <- df_FD$y  # Predicted_output is just the signal
        
        # par <- paste(predict.table$tool_name, predict.table$recipe_name, sep=":")
        # par <- paste(predict.table$tool_name, predict.table$traveler_step, sep=":")
        par <- paste(predict.table$tool_name, predict.table$recipe_name, sep=":")
        
#         a1 <- df_FD[,.N,by=list(tool_name, recipe_name)]
#         setkey(a1, tool_name, N)
#         a2 <- a1[,tail(.SD,3), by=tool_name]
#         par2 <- paste(a2$tool_name, a2$recipe_name, sep=":")
#         
#         i <- par %in% par2
#         predict.table <- predict.table[i,]
#         par1 <- par[i]
#         df.L <- split(predict.table, par1)
        
        df.L <- split(predict.table, par)# or par1
        # Calculate the remaining life per partition
        start.life <- 0
        end.life <- 1.3  #!!! User-defined threshold. May need to modify
        
        nruns <- 51
        data.list <- lapply(df.L, function(df) {
          # df <- predict.table 
          print("Dimension of each df: ")
          print(dim(df))
          df <- arrange(df, start_time)
          t <- round(df$start_time,"hours")
          
          # Remove all data before recent PM for this partition
          nam <- str_sub(df$tool_name[1], 1, 8) 
          df_pm1 <- df_pm[grepl(nam, df_pm$equip_id), ]
          
          # print("Error: Remember to on PM")
          if (nrow(df_pm1) > 0){
            # verify if the ETI data is actual event that affects sensor
            pm.time <- NULL
            for (i in 1:nrow(df_pm1)){
              tim <- df_pm1[i, equip_state_in_datetime]
              if (grepl("pfg", df_pm1$event_note_text[i], ignore.case = T) | grepl("filament", df_pm1$event_note_text[i], ignore.case = T)){
                pm.time <- tim
              } else {
                a <- which(difftime(df$start_time, tim) >0)
                # still 1 recipe has problem with delayed time record, so its pm not detected
                if ((length(a)>0) & (a[1] > 1)){
                  if (abs(df$predicted_output[a[1]] - df$predicted_output[a[1]-1])>0.2){
                  pm.time <- tim
                  }
                }   
              }
            }
           
            
            # pm.time <- tail(df_pm1$equip_state_in_datetime, 1)
            if (!is.null(pm.time)){
              t1 <- difftime(t, pm.time, units="hours")
              if (tail(df_pm1$equip_state_id,1) == "IN_PM") {
                df <- df[t1 > 24, ]  # allow a grace period of 24 hours / 100 points after PM or CM
              } else {
                df <- df[t1 > 1, ]
              } 
            #           df <- df[-c(1:100),]  # remove first 100 points
            }
            
            # check if there is step jump in the signal that > 0.2, if yes, cut off
            a <- c(0,diff(df$predicted_output))
            i <- which(abs(a) > 0.2)
            if (length(i) > 0){
              df <- df[i[length(i)]: nrow(df), ]
            }
            
            if (nrow(df) <= 50){
              print("Warning: not enough data after the grace period of PM/CM")
              return (NULL)
            }
          }
          
          hour <- difftime(sys.time, df$start_time, units="hours")
          idx <- which((hour >= 0) & (hour < no.hour.predict))  # look at wafers within (no.hour.predict) hours
          df1 <- df[idx,]
          # if there is no points of this partition within no.hour.predict, ignore it
          if (length(idx) == 0) {
            print("Warning: No data within latest 3 hours.")
            return (NULL)
          }
          
          # Calculate slope for each point within lastest 3 hours
          beg <- idx[1]
          nruns = max(nruns, beg)
          se = c()
          for (j in idx) {
            if (j >= nruns){
              se1 <- TrendCal(df$start_time[1:j], df[1:j, "predicted_output", drop=F],
                              type1="day", type2="slope", nWeeks=4/7, nruns = nruns)
              if (!is.na(se1) & se1 > 0){  # careful, this is to make all positive slope 0, applied for IM proj only
                se1 <- 0
              }
              se <- c(se, as.numeric(se1))
            }
          }
          # if there is not enough points to get trend in this partition, ignore it
          if (length(se) == 0 | all(is.na(se)) ) {
            print("Warning: Not enough data points to calculate slope. No prediction needed.")
            return (NULL)
          }
          # remove se NA or >= 0
          i <- which(!is.na(se) & se < 0)
          df1 <- df1[i,]
          se <- se[i]
          se <- sqrt(log(1+abs(se)))
          # For high current above 60, treat it as full life
          se[df1$predicted_output > 60] <- 0.12  # also let forecast give full life for current >= 60 later
          
          rem.life <- lapply(se, function(se){
            if ((start.life<=se & se<=end.life) | (start.life>=se & se>=end.life)){
              m <- 1 - (se-start.life)/(end.life-start.life)
            } else if ((se<start.life & start.life<end.life) | (se>start.life & start.life>end.life)){
              m <- 1
            } else if ((se>end.life & start.life<end.life) | (se<end.life & start.life>end.life)){
              m <- 0
            }
            m
          })
          df1$remaining_life <- as.numeric(rem.life)
          df1$performance_idx <- se
                  
          df1 <- df1[!is.na(df1$performance_idx),]
        })
        
        # Combine partitions to create predict.table
        # predict.table <- df1
        predict.table <- as.data.frame(rbindlist(data.list, use.names = T))  
        predict.table$end_life_thres <- rep(end.life, nrow(predict.table))
        predict.table <- arrange(predict.table, start_time)  # sort by start_time
        print("Dim of predict.table")
        print(dim(predict.table))
        if (nrow(predict.table) > 0){
          predict.table <- predict.table[,c("tool_name", "recipe_name", "traveler_step", "part_type", "lot_id", "wafer_id", "start_time",
                                            "predicted_output", "remaining_life",  "end_life_thres", "performance_idx", "signal")]
          predict.table$script_revision <- 4
          predict.table
        } else {
          predict.table
        }
      })
      
      # =====================================================
      # Write data file to local and HDFS
      print("Write table to HDFS ...")
      if (nrow(predict.table) == 0){
        warning("Predict.table has 0 rows.")
      }
      if (nrow(predict.table) > 0) {
        path <- paste0(local.path, "/test/predict")
        file.path <- paste0(path, "/", time.name, ".csv")
        header.path <- paste0(path, "/col_header/IM_col_header2.csv")
        success.path <- paste0(path, "/predict_success.csv")
        
        dir.create(paste0(path,"/col_header"), recursive=T)
        
        write.table(data.frame(col_header=names(predict.table)),
                    header.path, quote=F, row.names=F, sep=",", col.names=FALSE)
        write.table(format(predict.table, digits=12), 
                    file.path, quote=F, row.names=F, sep=",", col.names=FALSE)
        write.table(data.frame(a="Sury Chenbin Minh cheers :))"),
                    success.path, quote=F, row.names=F, sep=",", col.names=FALSE)
        
        # Upload table to hdfs
        hdfs.put(header.path, header.t2)
        hdfs.put(file.path, file.t2)
        hdfs.put(success.path, success.t) 
        
        # test1 <- read.csv(header.path, col.names="name", header=F)[, "name"]
        # test2 <- read.csv(file.path, col.names=test1, header=F)
      }
    }, error = function(e) {
      print(e)
      file.name <- paste0(time.name, "_predict_fail")
      path <- paste0(local.path, "/test/predict")
      success.path <- paste0(path, "/", file.name, ".csv")
      
      write.table(data.frame(a=c(args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], e)),
                  success.path, quote=F, 
                  row.names=c("no.hour.predict", "sys.time", "file.fd", "header.t2", "file.t2", "success.t", "source.path", "local.path", "pm.path", "error"), 
                  sep=",", col.names=FALSE)
      hdfs.put(success.path, success.t) 
    })
  }
}
