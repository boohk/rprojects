require(RPostgreSQL)

# Create a Connection
# db.info is a json format (line 158 - 170 참고)
ConnectDB <- function(db.info) {
  drv <- dbDriver("PostgreSQL")
  conn <- dbConnect(
    drv,
    user = as.character(db.info["user"]),
    password = as.character(db.info["password"]),
    dbname = as.character(db.info["dbname"]),
    host = as.character(db.info["host"]),
    port = as.character(db.info["port"])
  )
  return(conn)
}


# Get a Query Result
# conn: ConnectToPG(db.info)
GetResult <- function(conn, query){
  result <- dbGetQuery(conn, query)
  # on.exit(dbDisconnect(conn))
  return(result)
}


# Check a Schema Existence
# Whether a schema exist or not.
# it is used to CreateSchema, WriteTbl, DropSchema, DropTbl functions.
CheckSchemaExistence <- function(conn, schema.name) {
  query <-
    paste0("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '" ,
           schema.name,
           "')")
  exist.schema <- dbGetQuery(conn, query)
  return (exist.schema)
}


# Create Schema
# schema.name: it will be created using conn.
CreateSchema <- function(conn, schema.name) {
  exist.schema <- CheckSchemaExistence(conn, schema.name) 
  if(!exist.schema$exists[1]) {
    query <- paste("CREATE SCHEMA", schema.name)
    dbGetQuery(conn, query)
    # on.exit(dbDisconnect(conn))
    cat("schema", schema.name, "is created.", "\n")
  } else {
    cat("schema", schema.name, "already exists.", "\n")
  }
  rm(query, exist.schema)
}


# Create Table
# ow: Whether overwrite or not (TRUE, FLASE)
# check: for compare a existed table content to df. (defalut is TRUE)
# if ow is TRUE, overwrite to existed tbl.
WriteTbl <- function(conn, schema.name, tbl.name, df, ow, check=TRUE){
  exist.schema <- CheckSchemaExistence(conn, schema.name) 
  if(exist.schema$exists[1]) {
    # if a table not exist, write table.
    if(!dbExistsTable(conn, c(schema.name, tbl.name))){
      dbWriteTable(
        conn,
        c(schema.name, tbl.name),
        value = df,
        append = F,
        row.names = F
      )
      cat(paste(schema.name, tbl.name, sep="."), "is created.", "\n")
    } else {
      # when ow is True, even if a table exist, overwrite to existed tbl.
      if(ow) {
        dbWriteTable(
          conn,
          c(schema.name, tbl.name),
          value = df,
          overwrite = ow,
          row.names = F
        )
        cat(paste(schema.name, tbl.name, sep="."), "is created.", "\n")
      } else {
        cat("ow(overwrite) set", ow, "\n")
        # if not, firstly compare existed table to df.
        # And that is not perfetly same, return additional message.
        if(check){
          df_postgres <- dbReadTable(conn, c(schema.name, tbl.name))
          if(identical(df, df_postgres)) {
            cat(paste(schema.name, tbl.name, sep="."), "already exists!", "\n")
          } else {
            cat(paste(schema.name, tbl.name, sep="."), "already exists!", "\n")
            cat("But table contents is different")
          }
        } 
      }
    }
  } else {
    cat(schema.name, "doesn't exists.")
    cat("check schema first!", "\n")
  }
  rm(exist.schema)
}


# Delete a Schema
DropSchema <- function(conn, schema.name) {
  exist.schema <- CheckSchemaExistence(conn, schema.name) 
  if(exist.schema$exists[1]) {
    
    query <- paste0("select exists(SELECT count(*) FROM information_schema.tables ",
                    "WHERE table_schema = '", schema.name, "')")
    exist.tbls <- dbGetQuery(conn, query)
    if(exist.tbls$exists[1]) {
      cat(schema.name, "has table(s).")
      cat("So, it can't be dropped", "\n")
    } else {
      query <- paste("DROP SCHEMA", schema.name)
      result <- dbGetQuery(conn, query)
      cat(result, schema.name, "is dropped.", "\n")
    }
  }
}


# Delete aTable
DropTbl <- function(conn, schema.name, tbl.name) {
  exist.schema <- CheckSchemaExistence(conn, schema.name) 
  if(exist.schema$exists[1]) {
    if(dbExistsTable(conn, c(schema.name, tbl.name))){
      query <- paste("DROP TABLE", paste(schema.name, tbl.name, sep="."))
      result <- dbGetQuery(conn, query)
      cat(result, paste(schema.name, tbl.name, sep="."), "is dropped.", "\n")
    } else {
      cat(paste(schema.name, tbl.name, sep="."), "doesn't exists.")
    }
  } else {
    cat(schema.name, "doesn't exists.", "\n")
    cat("check schema first!", "\n")
  }
}


# Read a Table
ReadTbl <- function(conn, schema.name, tbl.name) {
  exist.schema <- CheckSchemaExistence(conn, schema.name)
  if(exist.schema$exists[1]) {
    if(dbExistsTable(conn, c(schema.name, tbl.name))) {
      result <- dbReadTable(conn, c(schema.name, tbl.name))
      return(result)
    } else {
      cat(tbl.name, "doesn't exists.")
    }
  } else {
    cat(schema.name, "doesn't exists.")
    cat("check schema first!", "\n")
  }
}


# Close All Connections
CloseConnections <- function(){
  conn.cnt <- length(dbListConnections(drv = dbDriver("PostgreSQL")))
  cat("Now Opened Connections:", conn.cnt, '\n')    
  
  lapply(dbListConnections(drv = dbDriver("PostgreSQL")), function(x) {dbDisconnect(conn = x)})
  conn.cnt <- length(dbListConnections(drv = dbDriver("PostgreSQL")))
  cat("Closed Connections", '\n')   
  cat("Now Opened Connections:", conn.cnt, '\n')
}
# CloseConnections()



####################################################

# "pg_info": {
#   "user": "input_user",
#   "password": "input_password",
#   "dbname": "input_dbname",
#   "host":"input_host",
#   "port":"input_port"
# }

# R에서 아래처럼 사용한다.
# file.path <- "data/db_info.json"
# pg.info <- jsonlite::fromJSON(file.path)$pg_info

####################################################
