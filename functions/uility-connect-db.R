GetDataFromOracle <- function(query) {
  
  # require(rJava)
  require(RJDBC)
  require(dplyr)
  # Sys.setenv(JAVA_HOME='/path/to/java_home')
  # options(java.parameters="-Xmx4g")
  
  
  # Output Java version
  .jinit()
  print(.jcall("java/lang/System", "S", "getProperty", "java.version"))
  
  jdbc.driver <- JDBC(driverClass="oracle.jdbc.OracleDriver",
                      classPath="../../../ojdbc7.jar")
  jsonlite::fromJSON("db_info.json")$oracle_info -> oracle.info
  jdbc.conn <- dbConnect(jdbc.driver,
                         url=as.character(oracle.info["url"]),
                         user=as.character(oracle.info["user"]),
                         password=as.character(oracle.info["password"]))
  
  # table <- dbListTables(jdbc.conn)
  # Query on the Oracle instance name.
  df <- dbGetQuery(jdbc.conn, query)
  
  # Close connection
  return(df)
  dbDisconnect(jdbc.conn)
  
}

GetDataFromPG <- function(query) {
  
  library(RPostgreSQL)
  jsonlite::fromJSON("db_info.json")$pg_info -> pg.info
  pg.conn <- dbConnect(PostgreSQL(),
                       user=as.character(pg_info["user"]),
                       password=as.character(pg_info["password"]),
                       dbname=as.character(pg_info["dbname"]),
                       host=as.character(pg_info["host"]),
                       port=as.character(pg_info["port"]))
  
  df <- dbGetQuery(pg.conn, query)
  return(df)
  
  # Close connection
  dbDisconnect(pg.conn)
}
