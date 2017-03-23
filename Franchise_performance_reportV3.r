closure<-read.csv("Consolidated_Dec_Closure.csv",stringsAsFactors = F)
handover<-read.csv("Consolidated_Dec_Handover.csv",stringsAsFactors = F)
dispatch<-read.csv("Consolidated_Dispatch_Dec.csv",stringsAsFactors = F)
all_months_data<- read.csv("all_months_load.csv",stringsAsFactors = F)
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
dispatch_count <- dispatch_new %>% group_by(sd,cn) %>% summarise(num_dispatch = length(Dispatch.ID)) %>% as.data.frame() 
handover_dispatch_both <- dispatch_count %>% full_join(handover_withpdd) %>% as.data.frame()

closure_new <- closure %>% select(sd = LastScanDate, wbn = Waybill, cn = DispatchCenter) %>% mutate(sd = (substring(sd,1,9))) %>% group_by(sd,cn) %>% summarise(shipments_closed= length(wbn))
handover_dispatch_closure_all <- closure_new %>% full_join(handover_dispatch_both) %>% as.data.frame()
handover_dispatch_closure_all <- handover_dispatch_closure_all %>% select(sd,cn,cod_handover,pickup_handover,prepaid_handover,total_handover,pdd_count,on_date_breach,overall_breach,num_dispatch) %>% as.data.frame()

write.csv(handover_dispatch_closure_all,file="Dec_franchise_performance.csv", row.names = F)

cns <- c("Bengaluru_KnktDFP_D (Karnataka)","Bengaluru_EjpraFNC_D (Karnataka)","Bengaluru_BKNgrDFP_D (Karnataka)","Delhi_VikasDFP_D (Delhi)")
xx <- all_months_data %>% filter(sl %in% cns) %>% mutate(sdate = substring(sd,14,23))
yy_last_scan_of_day <- xx %>% group_by(sdate,wbn,sl) %>% summarise(sd = max(sd)) %>% as.data.frame()
xx_filtered <- xx %>% inner_join(yy_last_scan_of_day) %>% arrange(wbn,sd)
xx_filtered<- xx_filtered %>% mutate(pending_or_not=ifelse(st %in% c("UD","PP") & ss %in% c("Pending","In Transit","Scheduled","Dispatched"),"Pending","Not Pending"))

final_result<- xx_filtered %>% group_by(sl,sdate) %>% summarise(total_pending=length(pending_or_not)) %>% as.data.frame()
final_result <- final_result %>% mutate(sdate= format(as.Date(sdate), "%m/%d/%Y"))
final_result<- final_result %>% select(sd = sdate, cn = sl,total_pending)
final_report<- handover_dispatch_closure_all %>% full_join(final_result) %>% as.data.frame()
