# 컬럼명을 대문자에서 소문자로 변경하는 함수
ChangeColnameUpperToLower <- function(df) {
  
  colnames.upper <- colnames(df)
  colnames.lower <- tolower(colnames.upper)
  colnames(df) <- colnames.lower
  rm(colnames.upper, colnames.lower)
  
  return(df)
  
}
