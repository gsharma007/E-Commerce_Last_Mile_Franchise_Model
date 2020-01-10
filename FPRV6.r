import datetime,os
#import psycopg2
#note that we have to import the Psycopg2 extras library!
#import psycopg2.extras
import sys
import urllib2
import json
os.chdir("/home/gaurav/Franchise_Performance_Reports/api_reports")
datetime.datetime(2017,3,28).strftime('%s')
x= datetime.datetime.today().strftime('%s')
y = int(x)/(24*3600)
sd = str(int((y-1)*24*3600 - 5.5*3600))
ed = str(int((y)*24*3600  - 5.5*3600))
cid_list=['IND560047AAB',"IND560062AAB","IND560022AAB","IND110018AAB"]
report_list=['hod','cd','dispatch','pdd']

i =0
j = 0
for i in range(len(report_list)):
    for j in range(len(cid_list)):
        
        #url="http://constellation.delhivery.com/api/report/hod?client=delhivery&sd=1490400000&ed=1490486400&cids=IND560047AAB"
        url="http://constellation.delhivery.com/api/report/"+report_list[i]+"?client=delhivery&sd="+sd+"&ed="+ed+"&cids="+cid_list[j]
        request=urllib2.Request(url,None,{'Content-Type':'application/json','Accept':'application/csv','Authorization':'token eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9'})
        resp=urllib2.urlopen(request)
        txt= resp.read()
        newfile = open(report_list[i]+".csv",'a')
        newfile.write(txt)
        newfile.close()

setwd("/home/gaurav/Franchise_Performance_Reports")
system(paste("cd",getwd()))
system("python api_data_fetch.py")
pdd <- read.csv("api_reports/pdd.csv",stringsAsFactors = F)
dispatch <- read.csv("api_reports/dispatch.csv",stringsAsFactors = F)
cd <- read.csv("api_reports/cd.csv",stringsAsFactors = F)
hod <- read.csv("api_reports/hod.csv",stringsAsFactors = F)
library(dplyr)
pdd <- pdd %>% filter(!Waybill %in% gsub("[.]"," ",names(pdd)), nchar(LastScanDate) > 0  ) %>% select(-LastScanDate) %>% unique()
dispatch <- dispatch %>% filter(!Center %in% gsub("[.]"," ",names(dispatch)), nchar(Close.Time) > 0) %>% unique()
cd <- cd %>% filter(!Waybill %in% gsub("[.]"," ",names(cd)), nchar(LastScanDate) > 0) %>%  unique()
hod <- hod %>% filter(!Waybill %in% gsub("[.]"," ",names(hod)), nchar(LastScanDate) > 0) %>% select(-LastScanDate) %>% unique() 

# pdd
int_names <- c("COD_Amount","DispatchCount")
for(i in int_names){
  pdd[,i] <- as.integer(pdd[,i])
}

# dispatch
names(dispatch)
int_names <- c("Delivered","Pending","Returned","Pickedup","Scheduled","Canceled","Collected","Total","Amount.Expected","Amount.Collected")
for(i in int_names){
  dispatch[,i] <- as.integer(dispatch[,i])
}


write.csv(hod,"handover.csv",row.names = F)
write.csv(cd,"closure.csv",row.names = F)
write.csv(pdd,"pending.csv",row.names = F)
write.csv(dispatch,"dispatch.csv",row.names = F)


## In all reports franchise centers filter is there

closure<-read.csv("closure.csv",stringsAsFactors = F)   ##fetching closure report from constellation dashboard
handover<-read.csv("handover.csv",stringsAsFactors = F) ##fetching handover report from constelltion dashboard
dispatch<-read.csv("dispatch.csv",stringsAsFactors = F) ##fetching dispatch report from constellation dashboard
pending<-read.csv("pending.csv",stringsAsFactors = F) ##pending shipments report from the HQ MongoDB

library(dplyr) 
handover_dc<- handover %>% group_by(DispatchCenter) %>% summarise(num_packages= n_distinct(Waybill)) %>% as.data.frame() ##Number of shipments handover in each Franchise DC if looking for single day
handover_new <- handover %>% select(wbn = Waybill, hd = HandOverDate, ss = Status, st = StatusType, pt = PackageType, cn = DispatchCenter, dispatch_count = DispatchCount, cs.sl = LastScanLocation, pdd = Promise.Date) %>% mutate(hd = (substring(hd,1,10)),pdd = (substring(pdd,1,10))) ##changing the name as per our nomenclature and taking dates only from 2 columns
handover_details <- handover_new %>% group_by(hd,cn,pt) %>% summarise(num_packages = n_distinct(wbn)) %>% as.data.frame() ##Number of shipments handover with filter of date,package type and dispatch center
library(reshape2)
handover_details <- dcast(handover_details,hd+cn~pt,value.var = "num_packages") %>% as.data.frame() ## Taking package type from columns to rows

nn <- names(handover_details)
nn <- setdiff(nn,c("hd","cn"))
handover_details$total <- 0 
handover_details[is.na(handover_details)] <- 0
for(i in nn){
  handover_details$total = handover_details$total + handover_details[,i]
} ##loop for calculating the sum of prepaid+cod for getting total

handover_details <- handover_details %>% select(sd= hd, cn, cod_handover = cod, pickup_handover = pickup, prepaid_handover = prepaid,total_handover = total)  ##changing the name of column headings

pdd_new <-  handover_new %>% group_by(pdd,cn) %>% summarise(num_packages = n_distinct(wbn),on_date_breach = sum(ifelse(dispatch_count == 0 & pdd == hd,1,0)),overall_breach = sum(ifelse(dispatch_count == 0 & pdd <= hd,1,0))) %>% as.data.frame() %>% filter(!is.na(pdd)) %>% select(sd = pdd, cn, pdd_count = num_packages,on_date_breach,overall_breach) ##Finding the PDD count first,then calculating breach shipments and at last changing column names as per nomenclature
handover_withpdd <- handover_details %>% full_join(pdd_new) %>% as.data.frame() #%>% inner_join(handover_details %>% select(sd)) %>% unique()


dispatch_new <- dispatch %>% select(cn = Center, sd = Creation.Time , Dispatch.ID , Delivered, Pending , Returned, Pickedup, Scheduled , Collected, Total, Canceled)
dispatch_new <- dispatch_new %>% mutate(sd = (substring(sd,1,10)))
dispatch_count <- dispatch_new %>% group_by(sd,cn) %>% summarise(num_dispatch = n_distinct(Dispatch.ID), shipments_dispatched = sum(Total,na.rm = T), shipments_delivered = sum(Delivered,na.rm = T), shipments_pending = sum(Pending,na.rm = T), shipments_pickedup = sum(Pickedup,na.rm = T), shipments_collected = sum(Collected,na.rm = T), shipments_canceled = sum(Canceled,na.rm = T), shipments_scheduled = sum(Scheduled,na.rm = T), shipments_returned = sum(Returned,na.rm = T), shipments_closed1 = sum(Delivered,na.rm = T) + sum(Pickedup,na.rm = T) , shipments_closed2 = sum(Delivered,na.rm = T)+sum(Returned,na.rm = T)+sum(Pickedup,na.rm = T)+sum(Collected,na.rm = T)) %>% as.data.frame() 

d_convert <- function(xx){
  #xx <- "2/10/2017"
  if(grepl("/",xx)){
    xx <- as.integer(unlist(strsplit(xx,"/")))
    return(as.character(format(as.Date(ISOdate(xx[1],xx[2],xx[3])),"%m/%d/%Y")))
  }
  if(grepl("-",xx)){
    xx <- as.integer(unlist(strsplit(xx,"-")))
    return(as.character(format(as.Date(ISOdate(xx[3],xx[1],xx[2])),"%m/%d/%Y")))
  }
}

dispatch_count <- dispatch_count %>% mutate(rowid = row_number()) %>% group_by(rowid) %>% mutate(sd = d_convert(sd)) %>% as.data.frame() %>% select(-rowid) %>% filter(!is.na(sd))
handover_withpdd <- handover_withpdd %>% mutate(rowid = row_number()) %>% group_by(rowid) %>% mutate(sd = d_convert(sd)) %>% as.data.frame() %>% select(-rowid) %>% filter(!is.na(sd))


handover_dispatch_both <- dispatch_count %>% full_join(handover_withpdd) %>% as.data.frame()

closure_new <- closure %>% select(sd = LastScanDate, wbn = Waybill, cn = DispatchCenter) %>% mutate(sd = (substring(sd,1,10))) %>% group_by(sd,cn) %>% summarise(shipments_closed= n_distinct(wbn)) %>% as.data.frame()
handover_dispatch_closure_all <- closure_new %>% full_join(handover_dispatch_both) %>% as.data.frame()
handover_dispatch_closure_all <- handover_dispatch_closure_all %>% select(sd,cn,cod_handover,pickup_handover,prepaid_handover,total_handover,pdd_count,on_date_breach,overall_breach,num_dispatch,shipments_dispatched,shipments_delivered,shipments_closed,shipments_pending,shipments_returned,shipments_pickedup,shipments_scheduled,shipments_canceled,shipments_collected,shipments_closed1,shipments_closed2) %>% as.data.frame() %>% filter(!grepl("[A-Z]",sd))

write.csv(handover_dispatch_closure_all,file="feb_franchise_performance.csv", row.names = F)

cns <- c("Bengaluru_KnktDFP_D (Karnataka)", "Bengaluru_EjpraFNC_D (Karnataka)","Bengaluru_BKNgrDFP_D (Karnataka)","Delhi_VikasDFP_D (Delhi)")
all_months_data <- read.csv("pp_scan.csv",stringsAsFactors = F)
xx <- all_months_data %>% filter(sl %in% cns) %>% mutate(sdate = substring(sd,14,23))
yy_last_scan_of_day <- xx %>% group_by(sdate,wbn,sl) %>% summarise(sd = max(sd)) %>% as.data.frame()
xx_filtered <- xx %>% inner_join(yy_last_scan_of_day) %>% arrange(wbn,sd)
xx_filtered<- xx_filtered %>% mutate(pending_or_not=ifelse(st %in% c("UD","PP") & ss %in% c("Pending","In Transit","Scheduled","Dispatched"),"Pending","Not Pending"))


final_result<- xx_filtered %>% group_by(sl,sdate) %>% summarise(total_pending=sum(ifelse(pending_or_not == "Pending",1,0))) %>% as.data.frame()
final_result <- final_result %>% mutate(sdate= format(as.Date(sdate), "%m/%d/%Y"))
final_result<- final_result %>% select(sd = sdate, cn = sl,total_pending)


d_convert <- function(xx){
  #xx <- "2/10/2017"
  if(grepl("/",xx)){
    xx <- as.integer(unlist(strsplit(xx,"/")))
    return(as.character(format(as.Date(ISOdate(xx[1],xx[2],xx[3])),"%m/%d/%Y")))
  }
  if(grepl("-",xx)){
    xx <- as.integer(unlist(strsplit(xx,"-")))
    return(as.character(format(as.Date(ISOdate(xx[3],xx[1],xx[2])),"%m/%d/%Y")))
  }
}


handover_dispatch_closure_all <- handover_dispatch_closure_all %>% mutate(rowid = row_number()) %>% group_by(rowid) %>%  mutate(sd= as.character(d_convert(sd))) %>% as.data.frame() %>% select(-rowid)
final_report<- handover_dispatch_closure_all %>% full_join(final_result) %>% as.data.frame()
final_report <- final_report %>% mutate(sd = as.Date(sd,format = "%m/%d/%Y"))
# xx_filtered %>% (ss,st)
# write.csv(xx_filtered,file= "xx_filtered.csv",row.names = F)
fr <- final_report %>% filter(sd == Sys.Date()-3) %>% filter(cn == "Bengaluru_BKNgrDFP_D (Karnataka)")
fr <- t(fr)
write.csv(fr,file= "final_result.csv",row.names = F)

system(paste("cd",getwd()))

"mongoexport -h 52.77.156.120 -d delhivery_db -c packages -f wbn,pdd,cn,s,cs.ss,cs.sd,cs.st,cs.sl -q '{\"cn\":{$in:[\"Bengaluru_KnktDFP_D (Karnataka)\",\"Bengaluru_EjpraFNC_D (Karnataka)\",\"Bengaluru_BKNgrDFP_D (Karnataka)\",\"Delhi_VikasDFP_D (Delhi)\"]},\"cs.sd\":{$lt:new Date(1445126400000),$gt:new Date(1444608000000)}}' --type=csv -o pp.csv -u ro_express -p 'x[td7R%;,'"

date_converter <- paste0(as.character(as.integer(as.POSIXct("2017-02-01 00:00:00"))),"000")
as.numeric(Sys.time())


end_time = as.numeric(Sys.time())
start_time = end_time - 24*3600
q <- paste0("mongoexport -h 52.77.156.120 -d delhivery_db -c packages -f wbn,pdd,cn,s,cs.ss,cs.sd,cs.st,cs.sl -q '{\"cn\":{$in:[\"Bengaluru_KnktDFP_D (Karnataka)\",\"Bengaluru_EjpraFNC_D (Karnataka)\",\"Bengaluru_BKNgrDFP_D (Karnataka)\",\"Delhi_VikasDFP_D (Delhi)\"]},\"cs.sd\":{$lt:new Date(",trunc(end_time),"000),$gt:new Date(",trunc(start_time),"000)}}' --type=csv -o pp.csv -u ro_express -p 'x[td7R%;,'")
system(q)
pp_scan_old <- read.csv("pp_scan.csv",stringsAsFactors = F)
system("python packagescan.py")
pp_scan <- read.csv("pp_scan.csv",stringsAsFactors = F)
pp_scan <- pp_scan %>% rbind(pp_scan_old) %>% unique()
write.csv(pp_scan,"pp_scan.csv",row.names = F)

source("pending_download_mongo.r")
source("api_data_fetch.r")
source("read_data.R")
source("processing.r")
system(paste("cd",getwd()))
system(paste("python sendmail.py",getwd()))

import smtplib,os
from sys import argv
script, wd = argv
pp = wd 
os.chdir(pp)
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders
fromaddr = "lmfranchise@delhivery.com"
#toaddr = ['srinivasan.s@delhivery.com','bhavneet.kaur@delhivery.com','arun.b@delhivery.com','bibin.thomas@delhivery.com','ei@delhivery.com','userx@delhivery.com','anurag.dua@delhivery.com','anirban.kundu@delhivery.com','atul.mathew@delhivery.com','himanshu.jethi@delhivery.com','pne.express@delhivery.com','pne.sm@delhivery.com']
#toaddr = ["bibin.thomas@delhivery.com","himanshu.jethi@delhivery.com","lakshminarayana.p@delhivery.com"]
toaddr = ["lakshminarayana.p@delhivery.com","g.sharma@delhivery.com","choudhary.divya@delhivery.com"]
#toaddr = ['srinivasan.s@delhivery.com','lakshminarayana.p@delhivery.com','vineet.singh@delhivery.com']

msg = MIMEMultipart()

msg['From'] = fromaddr
msg['To'] = ", ".join(toaddr)

from time import localtime, strftime
filename = strftime("%Y-%m-%d %H_%M_%S", localtime())
msg['Subject'] = "LM Franchise Performance Report - "+filename[:10]
body = "System generated mail. LM Franchise Performance Report" +str(open("final_report"+".csv", "rb").readlines())
msg.attach(MIMEText(body, 'plain'))
ftstamp=filename
# filename="reports"
# attachment = open(filename+".xlsx", "rb")
# filename = "LP_Metrics_" + ftstamp + ".xlsx"
# part = MIMEBase('application', 'octet-stream')
# part.set_payload((attachment).read())
# encoders.encode_base64(part)
# part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
# msg.attach(part)
filenames=["final_result"]
for filename in filenames:
    attachment = open(filename+".csv", "rb")
    filename = filename + ftstamp + ".csv"
    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
    msg.attach(part)

server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login(fromaddr, "delhivery123")
text = msg.as_string()
server.sendmail(fromaddr, toaddr, text)
server.quit()
