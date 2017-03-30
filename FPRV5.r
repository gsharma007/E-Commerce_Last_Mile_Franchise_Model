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
sd = str((y-1)*24*3600)
ed = str((y)*24*3600)
cid_list=['IND560047AAB',"IND560062AAB","IND560022AAB","IND110018AAB"]
report_list=['hod','cd','dispatch','pdd']

setwd("/home/gaurav/Franchise_Performance_Reports")
system(paste("cd",getwd()))
system("python api_data_fetch.py")
pdd <- read.csv("api_reports/pdd.csv",stringsAsFactors = F)
dispatch <- read.csv("api_reports/dispatch.csv",stringsAsFactors = F)
cd <- read.csv("api_reports/cd.csv",stringsAsFactors = F)
hod <- read.csv("api_reports/hod.csv",stringsAsFactors = F)
library(dplyr)
pdd <- pdd %>% filter(!Waybill %in% gsub("[.]"," ",names(pdd)), nchar(LastScanDate) > 0  ) %>% select(-LastScanDate) %>% unique()
dispatch <- dispatch %>% filter(!Center %in% gsub("[.]"," ",names(dispatch)), nchar(LastScanDate) > 0) %>% select(-LastScanDate) %>% unique()
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



closure<-read.csv("closure.csv",stringsAsFactors = F)
handover<-read.csv("handover.csv",stringsAsFactors = F)
dispatch<-read.csv("dispatch.csv",stringsAsFactors = F)
pending<-read.csv("pending.csv",stringsAsFactors = F)
library(dplyr)
handover_dc<- handover %>% group_by(DispatchCenter) %>% summarise(num_packages= n_distinct(Waybill)) %>% as.data.frame()
handover_new <- handover %>% select(wbn = Waybill, hd = HandOverDate, ss = Status, st = StatusType, pt = PackageType, cn = DispatchCenter, dispatch_count = DispatchCount, cs.sl = LastScanLocation, pdd = Promise.Date) %>% mutate(hd = (substring(hd,1,10)),pdd = (substring(pdd,1,10)))
handover_details <- handover_new %>% group_by(hd,cn,pt) %>% summarise(num_packages = n_distinct(wbn)) %>% as.data.frame()
library(reshape2)
handover_details <- dcast(handover_details,hd+cn~pt,value.var = "num_packages") %>% as.data.frame()
#Handover
nn <- names(handover_details)
nn <- setdiff(nn,c("hd","cn"))
handover_details$total <- 0
handover_details[is.na(handover_details)] <- 0
for(i in nn){
  handover_details$total = handover_details$total + handover_details[,i]
}
handover_details <- handover_details %>% select(sd= hd, cn, cod_handover = cod, pickup_handover = pickup, prepaid_handover = prepaid,total_handover = total)
pdd_new <-  handover_new %>% group_by(pdd,cn) %>% summarise(num_packages = n_distinct(wbn),on_date_breach = sum(ifelse(dispatch_count == 0 & pdd == hd,1,0)),overall_breach = sum(ifelse(dispatch_count == 0 & pdd <= hd,1,0))) %>% as.data.frame() %>% filter(!is.na(pdd)) %>% select(sd = pdd, cn, pdd_count = num_packages,on_date_breach,overall_breach)
handover_withpdd <- handover_details %>% full_join(pdd_new) %>% as.data.frame() #%>% inner_join(handover_details %>% select(sd)) %>% unique()
#Joining files
dispatch_new <- dispatch %>% select(cn = Center, sd = Creation.Time , Dispatch.ID , Delivered, Pending , Returned, Pickedup, Scheduled , Collected, Total, Canceled)
dispatch_new <- dispatch_new %>% mutate(sd = (substring(sd,1,10)))
dispatch_count <- dispatch_new %>% group_by(sd,cn) %>% summarise(num_dispatch = n_distinct(Dispatch.ID), shipments_dispatched = sum(Total), shipments_delivered = sum(Delivered), shipments_pending = sum(Pending), shipments_pickedup = sum(Pickedup), shipments_collected = sum(Collected), shipments_canceled = sum(Canceled), shipments_scheduled = sum(Scheduled), shipments_returned = sum(Returned), shipments_closed1 = sum(Delivered, Pickedup) , shipments_closed2 = sum(Delivered,Returned,Pickedup,Collected)) %>% as.data.frame() 
handover_dispatch_both <- dispatch_count %>% full_join(handover_withpdd) %>% as.data.frame()
closure_new <- closure %>% select(sd = LastScanDate, wbn = Waybill, cn = DispatchCenter) %>% mutate(sd = (substring(sd,1,10))) %>% group_by(sd,cn) %>% summarise(shipments_closed= n_distinct(wbn)) %>% as.data.frame()
handover_dispatch_closure_all <- closure_new %>% full_join(handover_dispatch_both) %>% as.data.frame()
handover_dispatch_closure_all <- handover_dispatch_closure_all %>% select(sd,cn,cod_handover,pickup_handover,prepaid_handover,total_handover,pdd_count,on_date_breach,overall_breach,num_dispatch,shipments_dispatched,shipments_delivered,shipments_closed,shipments_pending,shipments_returned,shipments_pickedup,shipments_scheduled,shipments_canceled,shipments_collected,shipments_closed1,shipments_closed2) %>% as.data.frame()
write.csv(handover_dispatch_closure_all,file="feb_franchise_performance.csv", row.names = F)


#Pendingshipments
cns <- c("Bengaluru_KnktDFP_D (Karnataka)","Bengaluru_EjpraFNC_D (Karnataka)","Bengaluru_BKNgrDFP_D (Karnataka)","Delhi_VikasDFP_D (Delhi)")
xx <- all_months_data %>% filter(sl %in% cns) %>% mutate(sdate = substring(sd,14,23))
yy_last_scan_of_day <- xx %>% group_by(sdate,wbn,sl) %>% summarise(sd = max(sd)) %>% as.data.frame()
xx_filtered <- xx %>% inner_join(yy_last_scan_of_day) %>% arrange(wbn,sd)
xx_filtered<- xx_filtered %>% mutate(pending_or_not=ifelse(st %in% c("UD","PP") & ss %in% c("Pending","In Transit","Scheduled","Dispatched"),"Pending","Not Pending"))
#Summarising
final_result<- xx_filtered %>% group_by(sl,sdate) %>% summarise(total_pending=sum(ifelse(pending_or_not == "Pending",1,0))) %>% as.data.frame()
final_result <- final_result %>% mutate(sdate= format(as.Date(sdate), "%m/%d/%Y"))
final_result<- final_result %>% select(sd = sdate, cn = sl,total_pending)

d_convert <- function(xx){
  xx <- "2/10/2017"
  xx <- as.integer(unlist(strsplit(xx,"/")))
  return(as.character(format(as.Date(ISOdate(xx[3],xx[1],xx[2])),"%m/%d/%Y")))
}
handover_dispatch_closure_all <- handover_dispatch_closure_all %>% mutate(sd= as.character(d_convert(sd)))
final_report<- handover_dispatch_closure_all %>% full_join(final_result) %>% as.data.frame()
xx_filtered %>% (ss,st)
write.csv(xx_filtered,file= "xx_filtered.csv",row.names = F)
write.csv(final_result,file= "final_result.csv",row.names = F)
