closure<-read.csv("closure.csv",stringsAsFactors = F)
handover<-read.csv("handover.csv",stringsAsFactors = F)
dispatch<-read.csv("dispatch.csv",stringsAsFactors = F)
pending<-read.csv("pending.csv",stringsAsFactors = F)
library(dplyr)
handover_new <- handover %>% select(wbn = Waybill, hd = HandOverDate, ss = Status, st = StatusType, pt = PackageType, cn = DispatchCenter, dispatch_count = DispatchCount, cs.sl = LastScanLocation, pdd = Promise.Date) %>% mutate(hd = as.Date(substring(hd,1,10),format = "%Y/%m/%d"),pdd = as.Date(substring(pdd,1,10),format = "%Y/%m/%d"))
handover_details <- handover_new %>% group_by(hd,cn,pt) %>% summarise(num_packages = n_distinct(wbn)) %>% as.data.frame()
library(reshape2)
handover_details <- dcast(handover_details,hd+cn~pt,value.var = "num_packages") %>% as.data.frame()

nn <- names(handover_details)
nn <- setdiff(nn,c("hd","cn"))
handover_details$total <- 0
handover_details[is.na(handover_details)] <- 0
for(i in nn){
  handover_details$total = handover_details$total + handover_details[,i]
}
handover_details <- handover_details %>% select(sd= hd, cn, cod_handover = cod, pickup_handover = pickup, prepaid_handover = prepaid,total_handover = total)

pdd_new <-  handover_new %>% group_by(pdd,cn) %>% summarise(num_packages = n_distinct(wbn),on_date_breach = sum(ifelse(dispatch_count == 0 & pdd == hd,1,0)),overall_breach = sum(ifelse(dispatch_count == 0 & pdd <= hd,1,0))) %>% as.data.frame() %>% filter(!is.na(pdd)) %>% select(sd = pdd, cn, pdd_count = num_packages,on_date_breach,overall_breach)

handover_details %>% full_join(pdd_new) %>% as.data.frame() #%>% inner_join(handover_details %>% select(sd)) %>% unique()


dispatch_count<- nrow(dispatch)
nrow(dispatch)
shipments_dispatched<- dispatch %>% summarise(total_dispatched=sum(as.numeric(Total)))
Shipments_delivered<- dispatch %>% summarise(total_delivered=sum(Delivered,Pickedup))
shipments_closed<- dispatch %>% summarise(total_closed=sum(Delivered,Pickedup,Returned,Collected))
handover <- handover %>% mutate(Promise.Date = as.POSIXct(strptime(Promise.Date,"%Y/%m/%d %H:%M:%S")))
handover$pdd<-substr(handover$Promise.Date,1,10)
pdd_count<- handover %>% select(Waybill,pdd) %>% group_by(pdd) %>% summarise(num_pdd= length(Waybill)) %>% as.data.frame()
pdd_breach<- handover %>% filter(pdd=="2017-03-09") %>% select(Waybill,DispatchCount) %>% group_by(DispatchCount) %>% summarise(breached_shipments=length(Waybill)) %>% as.data.frame()
finalreport<- 
