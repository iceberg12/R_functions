SetLib <- function(){
  ext_lib1 <- "/usr/lib64/Revo-7.3/R-3.1.1/lib64/R/library"
  ext_lib2 <- "/home/hdfswrite/R/x86_64-unknown-linux-gnu-library/3.1"
  if (! ext_lib1 %in% .libPaths()){
    .libPaths(c(.libPaths(), ext_lib1))
  }
  if (! ext_lib2 %in% .libPaths()){
    .libPaths(c(.libPaths(), ext_lib2))
  }
}

SetLib()

require(plyr)
require(dplyr)
require(tidyr)
require(stringr)
#require(pls)
require(useful)
require(zoo)
#require(glmnet)
#require(randomForest)
require(data.table)
#require(parallel)
require(foreach)
require(forecast)
# require(PMML)
# source("/home/etldata/POC_cases/C3/FDSumTranspose_DEVL_FINAL/scripts/withModel/VIP.R")

ReadFile <- function(file.path, col.names, ext,  header=F) { 
  result <- list() 
  i <- 1 
  
  # every time read 10000 lines
  handle <-hdfs.line.reader(file.path, 10000)
  on.exit(handle$close())  # ensure handle always closed even if error 
  
  # Importance: please use stringsAsFactors=F to avoid unwanted factor columns
  content <- handle$read() 
  if (header==T) {
    if (ext == "tsv") {
      result[[i]] <- read.delim(textConnection(content), header = F, stringsAsFactors=F, nrows=1)
    } else if (ext == "hive") {
      result[[i]] <- read.table(textConnection(content), header = F, stringsAsFactors=F, nrows=1, sep="\001")
    } else {
      result[[i]] <- read.csv(textConnection(content), header = F, stringsAsFactors=F, nrows=1)
    }
    i <- i + 1
  }
  while(length(content) != 0) {
    # Don't want to convert strings to factors, and it makes the code slightly faster 
    if (ext == "tsv") {
      result[[i]] <- read.delim(textConnection(content), col.names=col.names, header = F, stringsAsFactors=F) 
    } else if (ext == "hive") {
      result[[i]] <- read.table(textConnection(content), col.names=col.names, header = F, stringsAsFactors=F, sep="\001", comment.char="")
    } else {
      result[[i]] <- read.csv(textConnection(content), col.names=col.names, header = F, stringsAsFactors=F) 
    }
    
    content <- handle$read() 
    i <- i + 1 
  } 
  # Faster than do.call(rbind)
  rbindlist(result, use.names = T) 
} 

ReadFile1 <- function(file.path, ext, selCol=NULL) {   
  # Read header
  handle <- hdfs.line.reader(file.path, 1)
  content <- handle$read()
  if (ext == "tsv") {
    col.names <- unlist(str_split(content, "\t"))
  } else {
    col.names <- unlist(str_split(content, split=","))
  }
  handle$close()  # ensure handle always closed even if error
  
  # Choose certain columns to read in file
  if (length(selCol) > 0) {
    col.class <- rep("NULL", length(col.names))
    col.class[which(col.names %in% selCol)] <- NA
    selCol <- col.names[which(col.names %in% selCol)]
  } else {
    selCol <- col.names
    col.class <- rep(NA, length(col.names))
  }
  
  # Read file
  # Loop reading 10000 lines till the end.
  # Note that the headers have been read again in the first chunk, so have to remove later.
  # Please use stringsAsFactors=F to avoid unwanted factor columns and run faster.
  handle <- hdfs.line.reader(file.path, 10000)
  on.exit(handle$close())  # ensure handle always closed even if error 
  result <- list() 
  content <- handle$read()
  i <- 1
  while(length(content) != 0) {
    if (ext == "tsv") {
      result[[i]] <- read.delim(textConnection(content), header = F, stringsAsFactors=F, fill=T,
                                col.names=col.names, colClasses=col.class)
    } else {
      result[[i]] <- read.csv(textConnection(content), header = F, stringsAsFactors=F, fill=T,
                              col.names=col.names, colClasses=col.class) 
    }
    content <- handle$read() 
    i <- i + 1 
  }
  
  result[[1]] <- result[[1]][-1,] # Remove headers in the first line.
  df <- rbindlist(result, use.names=T)  # Combine data. Faster than do.call(rbind)
  setnames(df, selCol)
  df
} 

ReadFile2 <- function(local.path, header.path, file.path, ext, selCol=NULL){
  # Read header
  des <- paste0(local.path, "/", "download")
  dir.create(des, recursive=T)
  
  file <- tail(unlist(str_split(header.path, "/")), 1)
  file <- paste(des, file, sep="/")
  if (!file.exists(file)){
    hdfs.get(header.path, des)
    cat(1)
  }
  if (ext == "tsv") {
    col.names <- unlist(read.delim(file, header = F, stringsAsFactors=F))
  } else {
    col.names <- unlist(read.csv(file, header = F, stringsAsFactors=F))
  }
  
  # Choose certain columns to read in file
  if (length(selCol) > 0) {
    col.class <- rep("NULL", length(col.names))
    col.class[which(col.names %in% selCol)] <- NA
    selCol <- col.names[which(col.names %in% selCol)]
  } else {
    selCol <- col.names
    col.class <- rep(NA, length(col.names))
  }
  
  # library(readr) to read faster read_delim
  # Read file
  
  file <- tail(unlist(str_split(file.path, "/")), 1)
  file <- paste(des, file, sep="/")
  if (!file.exists(file)){
    hdfs.get(file.path, des)
    cat(2)
  }
  if (ext == "tsv") {
    df <- read.delim(file, header = F, stringsAsFactors=F, fill=T,
                     col.names=col.names, colClasses=col.class)
  } else {
    df <- read.csv(file, header = F, stringsAsFactors=F, fill=T,
                   col.names=col.names, colClasses=col.class) 
  }
  
  # Remove temp files
  # do.call(file.remove, list(list.files(des, full.names=T)))
  setnames(df, selCol)
  df
}

Restruct <- function(df){
  # Restructure data set to extract UVA data from Hadoop FD
  # 
  # Args:
  # df: data queried from Hadoop FD
  # 
  # Returns:
  # result: data frame with context and UVA data as columns
  
  # stopifnot(class(df) == "data.frame" or dim(df)[1] == 0 or dim(df)[2] == 0)
  
  name <- names(df)
  idx <- which(name == "window")
  result <- df %>%
    mutate(model = paste(window, sensor, statistic, sep=".")) %>% # recover FD model name
    select(c(2:(idx-2), model, result)) %>% # extract context and model and result data
    distinct(lot_id, wafer_id, model) %>%  # remove duplicated data rows
    spread(model, result)
}

Preprocess <- function(df, naRow=.2, naCol=.2, staThres=.9, ctx.name) {
  # Preprocess the data frame by NA processing
  # 
  # Args:
  # df: data frame
  # 
  # Returns:
  # result: a non-NA data frame
  
  # stopifnot(class(df) == "data.frame" or dim(df)[1] == 0 or dim(df)[2] == 0)
  
  df <- as.data.frame(df)
  ctx <- names(df) %in% ctx.name
  temp.data <- df[,colMeans(is.na(df)) <= naCol | ctx]  # Remove columns with more than naCol% NA
  temp.data <- temp.data[rowMeans(is.na(temp.data)) <= naRow,]  # Remove rows with more than naRow% NA
  temp.data <- colwise(na.locf)(temp.data)  # Fill up NAs with previous values
  # temp.data <- ddply(temp.data, function(x) replace(x, TRUE, lapply(x, na.locf)))
  temp.data <- na.omit(temp.data) 
  
  # Remove columns which has too many identical values (99%)
  f <- function(x) table(x)[[which.max(table(x))]]/length(x) > staThres
  iL <- which(apply(temp.data, 2, f))
  iL <- iL[! iL %in% which(ctx)]
  if (length(iL) != 0) {
    temp.data <- temp.data[,-iL]
  }
  
  result <- temp.data
}

RemoveCorVar <- function(df) {
  
  m <- which(cor(df) >= 0.95, arr.ind=T)
  m <- as.data.frame(m)
  m1 <- m[m[,1]<m[,2],]
  n <- arrange(m1, row)
  # df <- df[,-unique(n[,1])]
  
  # return the columns to be removed 
  # and the var-correlation table to check back using its unique columns
  list(unique(n[,1]), m)
}

Model <- function(df, yname, method, nf=5, ydist="gaussian", scale=T,
                  wgt=rep(1, nrow(df)), ncomp=12, alpha=1, ntree=50){
  # Return fitting model from inputs to the response
  # 
  # Args:
  # df: data frame as data.frame
  # yname: output name as string
  #
  # Return:
  # the fitting model
  
  set.seed(12)
  
  yidx <- which(names(df) %in% c(yname))
  formula <- build.formula(yname, names(df)[-yidx])
  dataX <- as.matrix(df[, -yidx])
  dataY <- as.matrix(df[, yidx])
  
  if (method == "pls") {
    cvsegments(nrow(df), nf, ceiling(nrow(df)/nf), type="interleaved")
    model <- plsr(formula, ncomp=ncomp, data = df, segments=nf,
                  method="oscorespls", scale=scale, validation="CV")
  }
  if (method == "cppls") {
    cvsegments(nrow(df), nf, ceiling(nrow(df)/nf), type="interleaved")
    model <- cppls(formula, ncomp=ncomp, data = df, segments=nf, weights=wgt,
                   scale=scale, validation="CV")
  }
  if (method == "glmnet") {
    #     cv <- list()  # check validation across all alpha
    #     for (i in 1:6) {
    #       cv[[i]] <- cv.glmnet(dataX, dataY, family='gaussian', weights=weight, nfold=n.fold, alpha=(i-1)/5)
    #     }
    model <- cv.glmnet(dataX, dataY, alpha=alpha, family=ydist, weights=wgt, nfold=nf)
  }
  if (method == "randomForest"){
    model <- randomForest(formula, data=df, ntree=ntree)
    
  }
  #   if (method == "cppls"){
  #     formula <- build.formula(yname, names(df)[-yidx])
  #     weight <- abs(df[, yname])
  #     model <- cppls(formula, ncomp=5, data = df, weights=weight, scale=T, validation = "CV")
  #   }
  result <- model
}

Screen <- function(df, yname, method, ncomp=12, alpha=1, ntree=50){
  # Return most influential variables that together affects the response
  # 
  # Args:
  # df: data frame as data.frame
  # y: output name as string
  #
  # Return:
  # names of the VIP variables and their ranking
  # pls -> weighted Variance Importance in Projection across components
  #
  # Note: 
  # 1. need to add carspls method later
  # 2. Response may be poisson, gaussian
  
  if (method == "pls"){
    model.screen <- Model(df, yname, method, ncomp=ncomp)
    vips <- VIP(model.screen)
    ncomp <- model.screen$ncomp
    vip.temp <- sapply(data.frame(vips),
                       function(x) c(ncomp:1)%*%x/(ncomp*(1+ncomp)/2))
    vip.temp <- sort(vip.temp, decreasing=T)
    # plot
    #     par(mfrow=c(2,1))
    #     n <- min(5, length(vip.temp))
    #     vip.i <- which(vip.temp %in% sort(vip.temp, decreasing=T)[1:n])
    #     xname <- names(vip.temp)
    #     xtitle <- paste0(vip.i[1], ": ", xname[vip.i[1]], "; ",
    #                      vip.i[2], ": ", xname[vip.i[2]], "; ",
    #                      vip.i[3], ": ", xname[vip.i[3]], "; ",
    #                      vip.i[4], ": ", xname[vip.i[4]], "; ",
    #                      vip.i[5], ": ", xname[vip.i[5]])
    #     matplot(vips[, vip.i], type=c("b"), xlim=c(0,ncomp), 
    #             xlab=xtitle, ylab="Variance Importance of Projection",
    #             pch=1, col=-1+1:length(vip.i), cex.lab=0.8) #plot
    #     legend("topleft", legend = vip.i, col=1:n, pch=1, cex=0.7) # optional legend
    #     
    #     
    #     validationplot(model.screen, legendpos="topright")
    #     par(mfrow=c(1,1))
  } 
  if (method == "glmnet") {
    model.screen <- Model(df, yname, method, alpha=alpha)
    #     vip.temp <- as.matrix(coef(model.screen, s = "lambda.min"))
    vip.temp1 <- coef(model.screen, s = "lambda.min")
    vip.temp2 <- coef(model.screen, s = "lambda.1se")
    vip.temp <- list(vip.temp1, vip.temp2)
    
    # plot 
    #     par(mfrow=c(2,1))
    #     plot(model.screen$glmnet.fit, xvar="lambda", label=T)  # plot MSE on log(lambda)
    #     abline(v=log(c(model.screen$lambda.min, model.screen$lambda.1se)), lty=2)
    #     plot(model.screen)  # plot coef shrinkage
    #     par(oma=c(0,0,2,0))
    #     title(main="Number of variables", outer=T)
    #     par(mfrow=c(1,1))
  }
  if (method == "randomForest"){
    model.screen <- Model(df, yname, method, ntree=ntree)
    vip.temp <- importance(model.screen)
    
    # plot
    #     par(mfrow=c(2,1))
    #     plot(model.screen, type="h", main="MSE vs. Number of trees")
    #     varImpPlot(model.screen, n.var=12, main="Variable Importance")
    #     par(mfrow=c(1,1))
  }
  result <- vip.temp # sort(vip.temp, decreasing=T)
}

Get.Model <- function(df, yname, method, train.pct=0.75){
  set.seed(12)
  idx <- sample(1:nrow(df), floor(train.pct*nrow(df)))
  train.data <- df[idx,]
  test.data <- df[-idx,]
  
  #   if (missing(weight)) {
  #     weight <- rep(1, nrow(df))
  #   }
  #   train.weight <- abs(train.data[, yname])
  
  model <- Model(train.data, yname, method)
}

TrendCal <- function(base, signals, type1, type2, nWeeks=1/7, percentRuns=0, nruns=200) {
  # Calculate the slope and also an drift index to determine if individual signal trends up
  #
  # Args:
  # start.time: a vector of time (as.POSIXct)
  # signals: a data frame with signals in columns (data.frame)
  # type1: "day" to calculate slope by days, "run" to calculate slope by runs (string)
  # type2: "slope" to calculate slope, "drift" to calculate drift index (string)
  
  # set weight for the nWeeks time frame/ recent runs (biweight kernel, EWMA, ...)
  wfunc <- function(x, h) rep(1,length(x))  #(1 - (x/h)^2)^2 # biweight kernel
  
  if (type1 == "day") {
    # sort according to time
    df <- data.frame(x=base, signals)
    dayLen <- 60 * 60 * 24   # number of seconds per day
    df$x <- as.numeric(df$x) / dayLen  # convert to day
    df <- df[order(df$x),]
    
    x <- df$x
    y <- as.data.frame(df[, -1])
  } else if (type1 == "run") {
    x <- base
    y <- signals
  }
  
  j = length(x)  
  # go through each column of signals to calculate the slope and drift index
  result <- sapply(seq_along(y), function(i) {
    # assign xi, yi according to type 1 (days or runs)
    if (type1 == "day") {  
      xEnd <- x[j]
      xBeg <- xEnd - nWeeks*7  # looking back nWeeks to calculate trend
      intvl <- which((x >= xBeg) & (x <= xEnd) & !is.na(y[,i]))
      if (length(intvl) <= 2) {
        return (list(0, 0, 0))
      }
      xi <- x[intvl]
      yi <- y[intvl,i]
      wi <- wfunc(xEnd - xi, nWeeks*7)
      # print(length(xi))
    } else if (type1 == "run") {
      if (percentRuns != 0) {
        if (percentRuns * j < 3) return (list(0, 0, 0))
        xi <- x[(floor(j - percentRuns*j) + 1):j]
        yi <- y[(floor(j - percentRuns*j) + 1):j,i]
      } else {
        xi <- x[max(1, j-nruns):j]
        yi <- y[max(1, j-nruns):j,i]
      }
      wi <- wfunc(xi, 1)
    }
    require(MASS)
    xi <- xi - mean(xi, na.rm=T)
    m <- rlm(yi~xi, data=data.frame(xi, yi), weights=wi)
    ySd <- sd(yi - m$fitted, na.rm=T) # residual std error
    b <- m$coefficients
    
    trendOut <- list(0,0,0)
    # 
    if((xi[length(xi)] - xi[1] < nWeeks*7/3) & type1=="day" |  
         (length(xi) < nruns) & (type1=="run") & (percentRuns==0)|
         (length(b) != 2) | 
         !(is.finite(b[2])) | 
         ySd < 0) {
      print(names(y)[i])
      print("Warning: In TrendCal, not enough point to get trend. Either day is not spread out of half a day, or not enough points to calculate trend.")
    } else if (b[2] == 0) {
      print(names(y)[i])
      print("Warning: No trend at all")
      trendOut[[3]] <- summary(m)$sigma
    } else {
      # slope
      trendOut[[1]] <- b[2]
      # drift index
      if (ySd == 0) {
        trendOut[[2]] <- 0
      } else{
        trendOut[[2]] <- abs(b[2])/ySd
      }
      trendOut[[3]] <- summary(m)$sigma
    }
    trendOut
  })
  
  result <- data.frame(result)
  
  if (result[1,] == 0 & result[2,]==0 & result[3,] == 0) {
    NA
  } else if (type2 == "slope") {
    result[1,]  
  } else if (type2 == "drift") {
    result[2,]
  } else if (type2 == "sigma") {
    result[3,]
  }
  
}

DriftDetection <- function(time.stamp, signals, drift.thres=0.1) {
  # Detect signals that drift
  # 
  # Args
  # time.stamp: a vector of time (as.POSIXct)
  # signals: data frame containing time signals in columns (data.frame)
  # drift.thres: a threshold for drift index to be compared (numeric) - adjustable
  #
  # Return
  # result: a vector of variable names
  
  
  # calculate drift index
  drift <- TrendCal(time.stamp, signals, type="drift")
  # return names of signals that are drifting
  names(signals)[which(drift > drift.thres)]
}

FocastLifeEnd <- function(start.time, signals, lifeEnd, nWeeks=1, percentRuns=1/4, type1, lifeBeg=1000) {
  # From the time series signals with the timestamp startTime,
  # predict how long until the signals with reach its lifeEnd.
  #
  # Args:
  # startTime: time stamp associated with the time series
  # signals: variable values
  # lifeEnd: the life threshold
  # nWeeks: no. of weeks to look back from the current time to calculate the slope, optional
  # percentRuns: percent of Runs to look back, optional.
  # type1: forecast life by "day" or by "run"
  #
  # Return:
  # eol: the amount of time in days until signals hits the threshold lifeEnd
  
  # predict remaining life by day
  if (type1 == "day") {
    trend <- rep(lifeBeg, 5)
    trend[1] <- TrendCal(start.time, signals=signals, type1="day", type2="slope", nWeeks=1*nWeeks)
    trend[2] <- TrendCal(start.time, signals=signals, type1="day", type2="slope", nWeeks=2*nWeeks)
    trend[3] <- TrendCal(start.time, signals=signals, type1="day", type2="slope", nWeeks=3*nWeeks)
    trend[4] <- TrendCal(start.time, signals=signals, type1="day", type2="slope", nWeeks=4*nWeeks)
    trend[5] <- TrendCal(start.time, signals=signals, type1="day", type2="slope", nWeeks=nWeeks)
    trend <- as.numeric(trend)  # break out from list
    
    eol <- rep(NA, 5)
    time <- as.numeric(start.time) / (60*60*24*7)  # convert to weeks
    for (i in 1:5) {
      if (!is.na(trend[i])){
        i.time <- which(time > tail(time,1) - i/7*3/24)  # look back i*3hours to average lifeCur
        lifeCur <- mean(signals[i.time, 1])
        if (lifeCur < lifeEnd) {
          eol[i] <- 0
        } else {
          if (trend[i] >= 0) {
            eol[i] <- sqrt(max(lifeCur,0)) * lifeBeg  # account for current status towards prediction
          } else {
            eol[i] <- sqrt(max(lifeCur,0)) * (lifeEnd - lifeCur) / trend[i]
            eol[i] <- min(eol[i], lifeCur * lifeBeg)
          }
        }  
      }
    }
  }
  
  # predict remaining life by run
  if (type1 == "run") {
    trend <- rep(0, 5)
    trend[1] <- TrendCal(start.time, signals=signals, type1="run", type2="slope", percentRuns=1/4)
    trend[2] <- TrendCal(start.time, signals=signals, type1="run", type2="slope", percentRuns=2/4)
    trend[3] <- TrendCal(start.time, signals=signals, type1="run", type2="slope", percentRuns=3/4)
    trend[4] <- TrendCal(start.time, signals=signals, type1="run", type2="slope", percentRuns=4/4)
    trend[5] <- TrendCal(start.time, signals=signals, type1="run", type2="slope", percentRuns=percentRuns)
    trend <- as.numeric(trend)  # break out from list to vector
    
    eol <- rep(0, 5)
    run <- 1:nrow(signals)
    for (i in 1:5) {
      i.run <- which(run > tail(run,1) * (1 - i/4/15))  # look back i/15 part of data to average lifeCur
      lifeCur <- mean(signals[i.run, 1])
      if (lifeCur < lifeEnd) {
        eol[i] <- 0
      } else {
        if (trend[i] >= 0) {
          eol[i] <- sqrt(max(lifeCur,0)) * lifeBeg
        } else {
          eol[i] <- sqrt(max(lifeCur,0)) * (lifeEnd - lifeCur) / trend[i]
          eol[i] <- min(eol[i], lifeCur * lifeBeg)
        }
      }  
    }
  }
  if (all(is.na(eol))) {
    return (NA)
  }
  eol <- sort(eol, decreasing = T)
  eol1 <- eol[1]
  lambda <- 0.5
  for (i in 1:length(eol)){
    eol1 <- lambda*eol1 + (1-lambda)*eol[i]
  }
  eol <- min(eol, na.rm=T)
  eol <- round(eol, digits=1)
}


ClassifyAbort <- function(df, method="svm", abortDate) {
  # Not implemented yet
}
