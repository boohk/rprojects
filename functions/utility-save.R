# excel sheets에 한번에 데이터 저장
save.to.xlsx<- function(df.list, df.names, save.path) {
  
  require(rJava)
  require(xlsx)
  
  i <- 1
  try({
    
    if(str_sub(save.path, -5, -1) != '.xlsx'){
      save.path <- paste0(save.path, ".xlsx")
    }
    
    while(i <= length(df.list)) {
      buff.df <- as.data.frame(df.list[i])
      buff.name <- toString(df.names[i])
      write.xlsx(buff.df,             # R데이터명
                 file=save.path,  # 여기서는 기존의 엑셀 파일이라고 설정함
                 sheetName=buff.name,  # 기존의 엑셀 파일에 new라는 시트에 데이터를 넣음
                 col.names=TRUE,   # 변수이름을 그대로 사용
                 row.names=FALSE,  # 행이름은 사용하지 않음
                 append=TRUE)      # 기존의 엑셀 파일이 있으면 그곳에 추가해서 저장
      i <- i + 1
      
    }
  })
}

# json 형태로 저장
save.to.json <- function(df, save.path) {
  
  require(jsonlite)
  require(stringr)
  
  try({
    
    if(str_sub(save.path, -5, -1) != '.json'){
      save.path <- paste0(save.path, ".json")
    } 
    
    # json.from.df <- toJSON(df)
    write_json(df, path=save.path)
    
  })
  
}

# rds 형태로 저장
save.to.rds <- function(df, save.path){
  require(stringr)
  require(readr)
  
  try({
    
    if(str_sub(save.path, -4, -1) != '.rds'){
      save.path <- paste0(save.path, ".rds")
    }
    
    write_rds(df, path=save.path)
    
  })
  
}

# tsv 형태로 저장
save.to.tsv <- function(df, save.path){
  require(stringr)
  require(readr)
  
  try({
    
    if(str_sub(save.path, -4, -1) != '.tsv'){
      save.path <- paste0(save.path, ".tsv")
    }
    
    write_tsv(df, path=save.path, quote=FALSE, col_names = TRUE, append = FALSE)
    
  })
  
}


