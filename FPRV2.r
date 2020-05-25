closure<-read.csv("Consolidated_Feb_Closure.csv",stringsAsFactors = F)
handover<-read.csv("Consolidated_Feb_Handover.csv",stringsAsFactors = F)
dispatch<-read.csv("Consolidated_Dispatch_Feb.csv",stringsAsFactors = F)
pending<-read.csv("pending.csv",stringsAsFactors = F)
library(dplyr)
handover_new <- handover %>% select(wbn = Waybill, hd = HandOverDate, ss = Status, st = StatusType, pt = PackageType, cn = DispatchCenter, dispatch_count = DispatchCount, cs.sl = LastScanLocation, pdd = Promise.Date) %>% mutate(hd = (substring(hd,1,9)),pdd = (substring(pdd,1,9)))
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
handover_withpdd <- handover_details %>% full_join(pdd_new) %>% as.data.frame() #%>% inner_join(handover_details %>% select(sd)) %>% unique()


dispatch_new <- dispatch %>% select(cn = Center, sd = Creation.Time , Dispatch.ID , Delivered, Pending , Returned, Pickedup, Scheduled , Collected, Total, Canceled)
dispatch_new <- dispatch_new %>% mutate(sd = (substring(sd,1,9)))
dispatch_count <- dispatch_new %>% group_by(sd,cn) %>% summarise(num_dispatch = length(Dispatch.ID), shipments_dispatched = sum(Total), shipments_delivered = sum(Delivered), shipments_pending = sum(Pending), shipments_pickedup = sum(Pickedup), shipments_collected = sum(Collected), shipments_canceled = sum(Canceled), shipments_scheduled = sum(Scheduled), shipments_returned = sum(Returned), shipments_closed1 = sum(Delivered, Pickedup) , shipments_closed2 = sum(Delivered,Returned,Pickedup,Collected)) %>% as.data.frame() 
handover_dispatch_both <- dispatch_count %>% full_join(handover_withpdd) %>% as.data.frame()

closure_new <- closure %>% select(sd = LastScanDate, wbn = Waybill, cn = DispatchCenter) %>% mutate(sd = (substring(sd,1,9))) %>% group_by(sd,cn) %>% summarise(shipments_closed= length(wbn))
handover_dispatch_closure_all <- closure_new %>% full_join(handover_dispatch_both) %>% as.data.frame()
handover_dispatch_closure_all <- handover_dispatch_closure_all %>% select(sd,cn,cod_handover,pickup_handover,prepaid_handover,total_handover,pdd_count,on_date_breach,overall_breach,num_dispatch,shipments_dispatched,shipments_delivered,shipments_pending,shipments_returned,shipments_pickedup,shipments_scheduled,shipments_canceled,shipments_collected,shipments_closed1,shipments_closed2) %>% as.data.frame()

write.csv(handover_dispatch_closure_all,file="feb_franchise_performance.csv", row.names = F)
