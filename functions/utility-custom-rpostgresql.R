require(RPostgreSQL)

###### DB Connection ###### 
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


###### Return Query Result ###### 
# Get a Query Result
# conn: ConnectToPG(db.info)
GetResult <- function(conn, query){
  result <- dbGetQuery(conn, query)
  # on.exit(dbDisconnect(conn))
  return(result)
}


###### Check  a schema and view table ###### 
# Check a Schema Existence
# Whether a schema exist or not.
# it is used to CreateSchema, WriteTbl, DropSchema, DropTbl functions.
CheckSchemaExistence <- function(conn, schema.name) {
  query <-
    paste0("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '" ,
           schema.name,
           "')")
  exist.schema <- dbGetQuery(conn, query)
  return(exist.schema)
}

# Check view or materialized view
# check if reuslt exists.
# defualt: view (not materialized view)
CheckViewTblExistence <- function(conn, schema.name, view.tbl.name, materialized=FALSE) {
  check.query = ""
  if(materialized) {
    check.query <- paste("SELECT EXISTS(",
                         "SELECT DISTINCT schemaname, matviewname FROM pg_matviews", 
                         paste0("where schemaname = '", schema.name, "' and matviewname = '", view.tbl.name, "')"))
    
  } else {
    check.query <- paste("SELECT EXISTS(",
                         "SELECT DISTINCT table_schema, table_name FROM INFORMATION_SCHEMA.COLUMNS", 
                         paste0("where table_schema = '", schema.name, "' and table_name = '", view.tbl.name, "')"))
    # view table은 SELECT * FROM pg_views where schemaname = 'schemaname' and viewname = 'viewname'이 더 나을 듯
    
  }
  exist.view.tbl <- dbGetQuery(conn, check.query)
  return(exist.view.tbl$exists[1])
}


###### Create a schema and table, view table ###### 
# Create Schema
# schema.name: it will be created using conn.
CreateSchema <- function(conn, schema.name) {

  exist.schema <- CheckSchemaExistence(conn, schema.name) 
  if(!exist.schema$exists[1]) {
    query <- paste("CREATE SCHEMA", schema.name)
    dbSendQuery(conn, query)
    if(CheckSchemaExistence(conn, schema.name)$exists[1]) {
      cat(schema.name, "schema", "is created.", "\n")
      rm(query, exist.schema)
    }
    # on.exit(dbDisconnect(conn))
  } else {
    cat(schema.name, "schema", "already exists.", "\n")
  }
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
# # if wanna append rows to existed tbl, just follow below.
# dbWriteTable(conn, name=c("data", "mall_category_info"), value=category201905, append=T, row.names=F, overwrite=F);

# Create view table
CreateViewTbl <- function(conn, schema.name, view.tbl.name, select.query, materialized=FALSE) {
  if(!is.logical(materialized)) {
    cat("A 'materialized' variable is a logical type, So need to convert it like TRUE(T) or FALSE(F).", "\n",
        "If materialized is TRUE, materialized view will be created.",
        "Or if it is False, just view will be created.", "\n")
    on.exit()
  } else {
    check.result <- CheckViewTblExistence(conn, schema.name, view.tbl.name)
    if (!check.result) {
      query = ""
      if(materialized) {
        pre_query <- "CREATE MATERIALIZED VIEW"
      } else {
        pre_query <- "CREATE VIEW"
      }
      create.view.query <- paste(pre_query, paste(schema.name, view.tbl.name, sep='.'), "as")
      full.query <- paste(create.view.query, select.query)
      rs <- dbSendQuery(conn, full.query)
      dbClearResult(rs)
    } else {
      if(!CheckSchemaExistence(conn, schema.name)) {
        cat(schema.name, "doesn't exists.", "\n")
      } else {
        cat("this materialized view table is already created.", "\n")
      }
    }
  }
}


###### Delete and Drop a schema and table, view table ###### 
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


# Delete a Table
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


# if materialized is TRUE, create a materialized view. if it is false, create a view.
DropViewTbl <- function(conn, schema.name, view.tbl.name, materialized=FALSE) {
  if(!is.logical(materialized)) {
    cat("A 'materialized' variable is a logical type, So need to convert it like TRUE(T) or FALSE(F).", "\n",
        "If materialized is TRUE, materialized view will be created.",
        "Or if it is False, just view will be created.", "\n")
  } else {
    exist.schema <- CheckSchemaExistence(conn, schema.name)
    if(exist.schema$exists[1]) {
    check.result <- CheckViewTblExistence(conn, schema.name, view.tbl.name, materialized)
    if (check.result) {
      query = ""
      if(materialized) {
        pre_query <- "DROP MATERIALIZED VIEW"
      } else {
        pre_query <- "DROP VIEW"
      }
      query <- paste(pre_query, paste(schema.name, view.tbl.name, sep="."))
      rs <- dbSendQuery(conn, query)
      dbClearResult(rs)
      cat(paste(schema.name, view.tbl.name, sep="."), "is dropped.", "\n")
    } else {
      cat(paste(schema.name, view.tbl.name, sep="."), "doesn't exists.")
    }
  } else {
    cat(schema.name, "doesn't exists.", "\n")
    cat("check schema first!", "\n")
   }
  }
}

###### Close All Connections ###### 
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


# Get table description : return table_name, column_name, data_type, character_maximum_length
# materialized: view = F, materialized view = T
GetTblDetail <- function(conn, tbl.name, materialized=FALSE) {
  query = ""
  if (materialized) {
    query <- paste0("select pg_class.relname AS table_name, pg_attribute.attname AS column_name, pg_catalog.format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS data_type ", 
                    "FROM pg_catalog.pg_class ", 
                    "INNER JOIN pg_catalog.pg_namespace ON pg_class.relnamespace = pg_namespace.oid ", 
                    "INNER JOIN pg_catalog.pg_attribute ON pg_class.oid = pg_attribute.attrelid ", 
                    "WHERE pg_class.relname  = '", tbl.name, "' and pg_class.relkind = 'm' AND pg_attribute.attnum >= 1 ", 
                    "ORDER BY table_name, column_name")
  } else {
    query <- paste0("select table_name, column_name, data_type ",
                    "from INFORMATION_SCHEMA.COLUMNS ",
                    "where table_name ='", tbl.name, "'")
  }
  result <- dbGetQuery(conn, query)  
  return(result)  
}

# Save all table description to xlsx format 
# tbl.names.vec: vector fomat like c("tbl_a, "tbl_b")
SaveAlltblDescAsXlsx <- function(tbl.names.vec, file_path, materialized) {
  total <-  data.frame()
  for (tbl.name in tbl.names.vec) {
    tbl.desc <- GetTblDetail(conn, tbl.name, materialized = materialized) %>% collect()
    total <- rbind(total, tbl.desc)
    rm(tbl.desc)
  }
  write.xlsx(total, file_path, col.names = T, row.names = F)
}

# GetAlltblDescAsXlsx(b, "tess_fucntion.xlsx", T)


############### DB Information Format ############### 

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

# change matview name
# query: ALTER MATERIALIZED VIEW schema.tbl_name RENAME TO new_tbl_name

# ##### DB가 느려질 땐, 쿼리로 확인을 #####
# # 출: https://semode.tistory.com/6

# pg.info <- jsonlite::fromJSON("data/db_info.json")$pg_info
# conn <- ConnectDB(pg.info)

# # 1) 현재 실행되고 는 쿼리 확인
# query0 = paste("SELECT datname, usename, state, query",
#                "FROM pg_stat_activity",
#                "WHERE state = 'active';")
# GetResult(conn, query0)

# # 2) 1분 이상 실행되는 쿼리 확인
# query1 = paste("SELECT pid, current_timestamp - query_start AS runtime, datname, usename, query",
#                "FROM pg_stat_activity",
#                "WHERE state = 'active' AND current_timestamp - query_start > '1 min'",
#                "ORDER BY 1 DESC;")
# GetResult(conn, query1)

# # 3) 현재 세션을 제외한 모든 세션 kill.
# query2 = paste("SELECT pg_terminate_backend(pid)",
#                "FROM pg_stat_activity",
#                "WHERE datname = current_database() AND pid <> pg_backend_pid();")
# GetResult(conn, query2)
# # 10분동안 유휴 상태인 세션 kill
# query3 = paste("SELECT pg_terminate_backend(8162)",
#                "FROM pg_stat_activity",
#                " WHERE state = 'idle in transaction' AND current_timestamp - query_start > '5 min'")
# GetResult(conn, query3)