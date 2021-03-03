library(insightscontextR)
library(DBI)
###########################
insightscontextR::hdfsClientInit()
hdfs.ls('.')
###########################Reading file from hdfs######################
file <- hdfs.file("/tmp/IntegrationTest01_T1445/integrationtest01.csv","r")
df <- hdfs.read(file)
df
#######################################################################

###########################Writing file back to hdfs###################
outfile <- hdfs.file("/tmp/IntegrationTest01_T1445/outputR/integrationtest01_output.csv", "w")
hdfs.write(object = df, con = outfile, hsync = T)
########################################################################

##########################hive connection###############################
connection <- insightscontextR::hiveConn()
table_list <- DBI::dbGetQuery(connection,'SHOW TABLES')
table_list
########################################################################

#######################creating a hive table############################
table <- data.frame(
  column1 = "five",
  column2 = "six"
)

tryCatch(
  # This is what I want to do...
  {
    DBI::dbWriteTable(
      conn = connection,
      name='testing_rhive_01',
      value = table,
      row.names = FALSE,
      field.types = c(
        column1 = "VARCHAR(50)",
        column2 = "VARCHAR(50)"
      )
    )
  },
  error=function(error_message){
    message(error_message)
  }
)

#######################reading a hive table############################
tabledata <- DBI::dbGetQuery(connection,'SELECT * FROM testing_rhive_01')
tabledata
########################inserting data to a hive table##################
tryCatch(
  # This is what I want to do...
  {
    DBI::dbSendQuery(connection, "INSERT INTO testing_rhive_01 VALUES ('three','four')")
  },
  error=function(error_message){
    message(error_message)
  }
)
#######################reading a hive table############################
tabledata <- DBI::dbGetQuery(connection,'SELECT * FROM testing_rhive_01')
tabledata
########################drop the hive table#############################
DBI::dbRemoveTable(connection, "testing_rhive_01")
########################################################################

####################sql connection######################################
library(RJDBC)
drv2 <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",
             "~/sqljdbc41.jar",
             identifier.quote="`")

conn <- dbConnect(drv2, "jdbc:sqlserver://10.2.4.4:1433;DatabaseName=C360", "pwcsqladmin", "Sgls02112017$")

ns <- dbSendQuery(conn, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES")

dbFetch(ns)

rs <- dbSendQuery(conn, "SELECT * FROM dbo.BattingData")
dbFetch(rs)

ns <- dbSendQuery(conn, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES")
dbFetch(ns)

dbSendUpdate(conn, "CREATE TABLE dbo.kerberos_qa (attribute1 INT, attribute2 INT)")
dbSendUpdate(conn, "INSERT INTO dbo.kerberos_qa (attribute1, attribute2) VALUES (1,2)")

ns <- dbSendQuery(conn, "SELECT * FROM dbo.kerberos_qa")
dbFetch(ns)

dbSendUpdate(conn, "DROP TABLE dbo.kerberos_qa")