# 단일 파일명 변경
# file.path: 파일 디렉토리
# file.name: 원래 파일명
# new.file.name: 변경할 파일명
ChangeFileName <- function(file.path, file.name, new.file.name) {
  buff.file.path <- file.path
  if(stringr::str_sub(buff.file.path, -1, -1) != '/') {
    buff.file.path <- paste0(buff.file.path, '/')
  }
  
  orig.file <- paste0(buff.file.path, file.name)
  new.file <- paste0(buff.file.path, new.file.name)
  
  if (file.exists(orig.file)) {
    file.rename(from = orig.file, to = new.file)
  }
  
}

# # example1
# base.path = "/home/datatf/users/hkbu/R/projects/functions/"
# file.list <- list.files(base.path, pattern = NULL, all.files = TRUE)
# ChangeFileName(base.path, file.list[5], "utility-naming.R")
# cat(1, list.files(base.path, pattern = NULL, all.files = TRUE), '\n')


# 조건에 맞는 전체 파일명 변경
# file.path: 파일 디렉토리
# orig.pattern: 파일명에서 변경할 패턴(?)
# new.pattern: 파일명에서 변경될 패턴(?)
ChangeFileNames <- function(file.path, orig.pattern, new.pattern) {
  require(stringr)
  
  file.list <- list.files(file.path, pattern = orig.pattern, all.files = TRUE)
  for (file.name in file.list){
    new.file.name <- str_replace(file.name, orig.pattern, new.pattern)
    ChangeFileName(file.path, file.name, new.file.name)
  }
}

# #example2
# base.path = "projects/functions/"
# cat(1, list.files(base.path, pattern = NULL, all.files = TRUE), '\n')
# ChangeFileNames(base.path, "Copy", "Cp")
# cat(2, list.files(base.path, pattern = NULL, all.files = TRUE), '\n')
# ChangeFileNames(base.path, "Cp", "Copy")
# cat(3, list.files(base.path, pattern = NULL, all.files = TRUE), '\n')


# 단일 파일 제거 
# file.path: 파일 디렉토리
# file.name: 제거할 파일명
DeleteFile <- function(file.path, file.name) {
  buff.file.path <- file.path
  if(stringr::str_sub(buff.file.path, -1, -1) != '/') {
    buff.file.path <- paste0(buff.file.path, '/')
  }
  file.name <- paste0(buff.file.path, file.name)
  
  if (file.exists(file.name)) {
    #Delete file if it exists
    file.remove(file.name)
  }
}

# 조건에 맞는 전체 파일 제거
# file.path: 파일 디렉토리
# name.pattern: 제거할 파일명 조건 패턴
DeleteFiles <- function(file.path, name.pattern) {
  file.list <- list.files(file.path, pattern = name.pattern, all.files = TRUE)
  for (file.name in file.list){
    DeleteFile(file.path, file.name)
  }
}

# #example3
# DeleteFile(file.path, "tmp.RData")
# DeleteFiles(file.path, "tmp")