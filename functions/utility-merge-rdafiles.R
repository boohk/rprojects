# Custom 필요!
# name.pattern: 저장할 Rdata 파일명에 포함된 공통된 pattern
# new.file.name: 새롭게 저장할 파일명
MergeRdatas <- function(file.path, name.pattern, new.file.name) {
  require(stringr)
  
  buff.file.path <- file.path
  if(str_sub(buff.file.path, -1, -1) != '/') {
    buff.file.path <- paste0(buff.file.path, '/')
  }
  
  file.list <- list.files(buff.file.path, pattern = name.pattern)

  a.df <- NULL
  b.df <- NULL
  c.df <- NULL
  
  time <- 1
  for (file.name in file.list){
    load(paste0(file.path, file.name))
    a.df <- rbind(a.df, open1.df)
    b.df <- rbind(b.df, open2.df)
    c.df <- rbind(c.df, open3.df)
    time <- time + 1
  }
  
  full.file.name <- paste0(file.path, new.file.name)
  save(file = full.file.name,
       a.df,
       b.df,
       c.df)
  cat("Merged", time, "files", '\n')
  cat("Saved here:", full.file.name, '\n')
  
  }

# #example
# file.path = getwd()
# MergeRdatas(file.path, "result.RData", "stat-result.RData")