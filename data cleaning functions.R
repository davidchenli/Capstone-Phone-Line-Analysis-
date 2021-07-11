rm(list=ls())
library(data.table)
library(readr)
library(plyr)
library(dplyr)
setwd("/Users/xiaotian/Desktop/project")
#dir<-"/Users/xiaotian/Desktop/LandPro Data/TPX Weekly Reports"
#file_list<-list.files(path = dir, pattern = "*.csv$",recursive = TRUE,full.names = TRUE)
#write.csv(ldply(file_list, read.csv, header=TRUE),"alldata.csv")
#OR
#df<-ldply(file_list, read.csv, header=TRUE)
df<-read.csv('alldata.csv')
df$X<-NULL

clean<-function(df){
  df = data.table(df)
  df$Answered[df$Answered=='Yes']=1
  df$Answered[df$Answered=='No'|df$Answered=='Redirect']=0
  df$Answered=as.numeric(df$Answered)
  
  with_blank=unique(df$extTrackingID[df$Department == ''])
  df<-df[df$extTrackingID %in% with_blank]
  
  inbound=unique(df$extTrackingID[df$Direction == "Inbound"])
  
  df$DateStamp = as.POSIXct(df$DateStamp,format="%m/%d/%Y %H:%M")
  df$DateStamp = format(df$DateStamp,"%m/%d/%Y/%H")
  
  a=df[df$extTrackingID %in% inbound,]
  b=a[a$Department != '' & a$Direction =="Outbound",]
  voice<-unique(df$extTrackingID[df$FirstName=="Voice"])
  c<-df[df$extTrackingID %in% voice,]
  c<-c[c$Department!="" & c$Direction=="Outbound",]
  d<-c[duplicated(c$extTrackingID),]
  f<-c[!c$extTrackingID %in% d$extTrackingID,]
  df_voice<-aggregate(x=list(NumD=f$Department),
                      by=list(extTrackingID=f$extTrackingID,DateStamp=f$DateStamp,Group=f$Group),
                      FUN=length)
  df_voice$missing<-1
  
  df=df[!df$extTrackingID %in%  b$extTrackingID,]
  df<-df[df$extTrackingID %in% inbound,]
  
  df_report<-aggregate(x=list(NumD=df$Department),
                       by=list(extTrackingID=df$extTrackingID,DateStamp=df$DateStamp,Group=df$Group),
                       FUN=length)
  by_region=df_report$extTrackingID[duplicated(df_report$extTrackingID)]
  df_test1=df[df$extTrackingID %in% by_region]
  df_test2 = df_test1[,.(Sum=(sum(Answered)),DateStamp=unique(DateStamp)),.(extTrackingID,Group)]
  
  com=rbind(df_test1[FirstName =="Voice",.(DateStamp,extTrackingID,Group)],df_test2[Sum == 0,.(DateStamp, extTrackingID,Group)])
  df_test2$missing=0
  df_test2$missing[paste(df_test2$extTrackingID,df_test2$Group) %in% paste(com$extTrackingID,com$Group)]=1
  
  df_report1=df[!df$extTrackingID %in% by_region & !df$extTrackingID %in% voice]
  df1.1=df_report1[,.(yes=sum(Answered),len=length(Answered)),.(tracking=extTrackingID)]
  df_report2 = df_report1[df_report1$extTrackingID %in% df1.1$tracking[df1.1$yes<2&df1.1$len>1]]
  
  df_report3<-df_report[!df_report$extTrackingID %in% by_region& !df_report$extTrackingID %in% voice,]
  df_report3$missing<-ifelse(df_report3$extTrackingID%in%df_report2$extTrackingID,1,0)
  
  df_report_final1 = rbind(df_report3[,c(1,2,3,5)],df_test2[,c(1,4,2,5)],df_voice[,c(1,2,3,5)])
  
  df_report_final1<-data.table(df_report_final1)
  
  df_report_final1 <- df_report_final1[,.(DateStamp=max(DateStamp)), .(extTrackingID=extTrackingID, Group=Group, missing=missing)]
  
  a<-df_report[df_report$NumD==1,]
  b<-df[df$extTrackingID %in% a$extTrackingID&df$Answered==0,]
  df_report_final1$missing[df_report_final1$extTrackingID %in% b$extTrackingID]=1
  write.csv(df_report_final1,"df_report_final1.csv")
}

clean2<-function(df){
  df = data.table(df)
  df$Answered[df$Answered=='Yes']=1
  df$Answered[df$Answered=='No'|df$Answered=='Redirect']=0
  df$Answered=as.numeric(df$Answered)
  inbound<-unique(df$extTrackingID[df$Direction == "Inbound"])
  df<-df[df$extTrackingID %in% inbound,]
  df$DateStamp = as.POSIXct(df$DateStamp,format="%m/%d/%Y %H:%M")
  df$DateStamp = format(df$DateStamp,"%m/%d/%Y/%H")
  with_blank=unique(df$extTrackingID[df$Department == ''])
  
  voice<-unique(df$extTrackingID[df$FirstName=="Voice"])
  c<-df[df$extTrackingID %in% voice,]
  c1<-c[Direction=="Inbound" & Department != "",.(length(Group)),.(Department, extTrackingID)]
  c2<-c[Direction=="Inbound" & Department != "",.(Answered=sum(Answered), LocalID=max(LocalID), DateStamp=max(DateStamp)),.(Department,extTrackingID)]
  voice_list<-c2$extTrackingID[duplicated(c2$extTrackingID)]
  c3<-c2[, .(max(LocalID)), extTrackingID]
  c2$voiceportal<-ifelse(c2$extTrackingID %in% c3$extTrackingID & c2$LocalID %in% c3$V1, 1, 0)
  
  c4<-c2[c2$voiceportal==0 & c2$Answered==1,]
  c5<-df[df$extTrackingID %in% c4$extTrackingID & df$Answered==1 & df$Department!="" & df$Direction!="Outbound",]
  c4<-merge(c4,c5[,c(4,6,7,25)],by=c("extTrackingID","Department"),x.all=T)
  c4$Name<-paste(c4$FirstName,c4$LastName)
  c4$FirstName<-NULL
  c4$LastName<-NULL
  #  c4$Name<-ifelse(c4$extTrackingID%in%c5$extTrackingID, paste(c5$FirstName,c5$LastName), 0)
  
  c6<-merge(c2[,c(1,2,3,5,6)],c4[,c(1,2,7)],by=c("Department","extTrackingID"),all=T)
  c6$Name[c6$voiceportal==1]<-"Voice Portal"
  
  c7<-c[c$Department!=""&c$Direction=="Outbound",]#
  d<-c7[duplicated(c7$extTrackingID),] #inter =1
  c6$Inter<-ifelse(c6$extTrackingID%in%d$extTrackingID, 1, 0)
  c6$Answered[c6$Name=="Voice Portal"]<-0
  
  df<-df[!df$extTrackingID%in%voice,]
  df1<-df[!df$extTrackingID %in% with_blank,]
  inter=unique(df$extTrackingID[df$Direction == "Outbound" & df$Department != ''])
  df2<-df[df$extTrackingID %in% df1$extTrackingID | df$extTrackingID %in% inter,]
  df4<-df2[df2$Direction!="Outbound"&df2$Department != "",]
  df4$Name<-paste(df4$FirstName,df4$LastName)
  #Answered or not
  df5<-df4[,.(DateStamp=max(DateStamp), Answered=sum(Answered)),.(Department,extTrackingID)]
  df6<-df4[Answered==1, .(Name, extTrackingID, Department)]
  df6<-distinct(df6)
  df7<-merge(df5,df6, by=c('extTrackingID','Department'),all=T)
  df7$Name[is.na(df7$Name)]<-' '
  df7$Answered<-ifelse(df7$Answered>0,1,0)
  df7$Inter<-1
  
  df8<-df[!df$extTrackingID %in% df2$extTrackingID,]
  df9<-df8[df8$Direction=="Inbound" & df8$Department!="",]
  df9$Name<-paste(df9$FirstName,df9$LastName)
  df10<-df9[,.(DateStamp=max(DateStamp), Answered=sum(Answered)),.(Department,extTrackingID)]
  df11<-df9[Answered==1, .(Name, extTrackingID, Department)]
  df11<-distinct(df11)
  df12<-merge(df10,df11, by=c('extTrackingID','Department'),all=T)
  df12$Name[is.na(df12$Name)]<-' '
  df12$Answered<-ifelse(df12$Answered>0,1,0)
  df12$Inter<-0
  df_report_final2<-rbind(df7,df12)
  df_report_final2<-rbind(df_report_final2,c6[,c(2,1,3,4,6,7)])
  a<-df[df$Direction=="Outbound"&df$Answered==1&df$Department==""&duplicated(df$extTrackingID),]
  b<-df[df$extTrackingID %in% a$extTrackingID & Direction=="Inbound" & Department != "",.(sum=sum(Answered)),extTrackingID]
  c<-b[b$sum==0,]
  c<-c[!c$extTrackingID %in% voice]
  d<-df[df$extTrackingID%in%c$extTrackingID,]
  
  #Sales, Admin, Service, Parts. Warranty
  e<-d[d$Direction=='Outbound'&d$Answered==1&d$Department=="",]
  f<-e[e$Caller.ID%like%"Sales"|e$Caller.ID%like%"Admin"|e$Caller.ID%like%"Service"|e$Caller.ID%like%"Parts"|e$Caller.ID%like%"Warrenty",]
  f$Department[f$Caller.ID%like%"Sales"]<-"Sales"
  f$Department[f$Caller.ID%like%"Admin"]<-"Admin"
  f$Department[f$Caller.ID%like%"Service"]<-"Service"
  f$Department[f$Caller.ID%like%"Parts"]<-"Parts"
  f$Department[f$Caller.ID%like%"Warrenty"]<-"Warrenty"
  f$Department<-paste(f$Department," (", f$Group, ")",sep="")
  f<-f[!duplicated(f$extTrackingID),]
  f<-f[,.(Department,Caller.ID,extTrackingID,DateStamp, Answered)]
  
  f$Inter<-ifelse(f$extTrackingID %in%df_report_final2$extTrackingID[df_report_final2$Inter==1], 1,0)
  
  
  #f<-e[e$Caller.ID=="HG ALL" | e$Caller.ID=="",]
  #liste<-e$extTrackingID[!e$extTrackingID%in%f$extTrackingID & !e$LocalID%in%f$LocalID]
  df_report_final2$Answered[paste(df_report_final2$Department,df_report_final2$extTrackingID)%in% paste(f$Department,f$extTrackingID)]<-1
  df_report_final2<-merge(df_report_final2, f, by=c("extTrackingID", "Department"),all=T)
  df_report_final2$Caller.ID<-ifelse(is.na(df_report_final2$Caller.ID), df_report_final2$Name, df_report_final2$Caller.ID)
  df_report_final2$DateStamp.x<-ifelse(!is.na(df_report_final2$DateStamp.y),df_report_final2$DateStamp.y,df_report_final2$DateStamp.x)
  df_report_final2$Answered.x<-ifelse(!is.na(df_report_final2$Answered.y),df_report_final2$Answered.y,df_report_final2$Answered.x)
  df_report_final2$Inter.x<-ifelse(!is.na(df_report_final2$Inter.y),df_report_final2$Inter.y,df_report_final2$Inter.x)
  df_report_final2$Name<-NULL
  df_report_final2$DateStamp.y<-NULL
  df_report_final2$Answered.y<-NULL
  df_report_final2$Inter.y<-NULL
  colnames(df_report_final2)<-c("extTrackingID", "Department","DateStamp","Answered","Inter", "Name")
  
  g<-e[!e$extTrackingID%in%f$extTrackingID,]
  h<-df_report_final2[df_report_final2$extTrackingID%in%g$extTrackingID,]
  h$Department<-"Unknown"
  h$Name<-"Unknown"
  h$Answered<-1
  h<-h[!duplicated(h$extTrackingID)]
  
  df_report_final2<-rbind(df_report_final2,h)
  df_report_final2$Name[is.na(df_report_final2$Name)]<-" "
  write.csv(df_report_final2, "df_report_final2.csv")
}

clean(df)
clean2(df)


# clean2<-function(df,i){
#   df<-read.csv(df)
#   df = data.table(df)
#   df$Answered[df$Answered=='Yes']=1
#   df$Answered[df$Answered=='No'|df$Answered=='Redirect']=0
#   df$Answered=as.numeric(df$Answered)
#   inbound<-unique(df$extTrackingID[df$Direction == "Inbound"])
#   df<-df[df$extTrackingID %in% inbound,]
#   df$DateStamp = as.POSIXct(df$DateStamp,format="%m/%d/%Y %H:%M")
#   df$DateStamp = format(df$DateStamp,"%m/%d/%Y/%H")
#   with_blank=unique(df$extTrackingID[df$Department == ''])
#   
#   voice<-unique(df$extTrackingID[df$FirstName=="Voice"])
#   c<-df[df$extTrackingID %in% voice,]
#   c1<-c[Direction=="Inbound" & Department != "",.(length(Group)),.(Department, extTrackingID)]
#   c2<-c[Direction=="Inbound" & Department != "",.(Answered=sum(Answered), LocalID=max(LocalID), DateStamp=max(DateStamp)),.(Department,extTrackingID)]
#   voice_list<-c2$extTrackingID[duplicated(c2$extTrackingID)]
#   c3<-c2[, .(max(LocalID)), extTrackingID]
#   c2$voiceportal<-ifelse(c2$extTrackingID %in% c3$extTrackingID & c2$LocalID %in% c3$V1, 1, 0)
#   
#   c4<-c2[c2$voiceportal==0 & c2$Answered==1,]
#   c5<-df[df$extTrackingID %in% c4$extTrackingID & df$Answered==1 & df$Department!=""&df$Direction!="Outbound",]
#   c4$Name<-ifelse(c4$extTrackingID%in%c5$extTrackingID, paste(c5$FirstName,c5$LastName), 0)
#   
#   c6<-merge(c2[,c(1,2,3,5,6)],c4[,c(1,2,7)],by=c("Department","extTrackingID"),all=T)
#   c6$Name[c6$voiceportal==1]<-"Voice Portal"
#   
#   c7<-c[c$Department!=""&c$Direction=="Outbound",]#
#   d<-c7[duplicated(c7$extTrackingID),] #inter =1
#   c6$Inter<-ifelse(c6$extTrackingID%in%d$extTrackingID, 1, 0)
#   c6$Answered[c6$Name=="Voice Portal"]<-0
#   
#   df<-df[!df$extTrackingID%in%voice,]
#   df1<-df[!df$extTrackingID %in% with_blank,]
#   inter=unique(df$extTrackingID[df$Direction == "Outbound" & df$Department != ''])
#   df2<-df[df$extTrackingID %in% df1$extTrackingID | df$extTrackingID %in% inter,]
#   df4<-df2[df2$Direction!="Outbound"&df2$Department != "",]
#   df4$Name<-paste(df4$FirstName,df4$LastName)
#   #Answered or not
#   df5<-df4[,.(DateStamp=max(DateStamp), Answered=sum(Answered)),.(Department,extTrackingID)]
#   df6<-df4[Answered==1, .(Name, extTrackingID, Department)]
#   df6<-distinct(df6)
#   df7<-merge(df5,df6, by=c('extTrackingID','Department'),all=T)
#   df7$Name[is.na(df7$Name)]<-' '
#   df7$Answered<-ifelse(df7$Answered>0,1,0)
#   df7$Inter<-1
#   
#   df8<-df[!df$extTrackingID %in% df2$extTrackingID,]
#   df9<-df8[df8$Direction=="Inbound" & df8$Department!="",]
#   df9$Name<-paste(df9$FirstName,df9$LastName)
#   df10<-df9[,.(DateStamp=max(DateStamp), Answered=sum(Answered)),.(Department,extTrackingID)]
#   df11<-df9[Answered==1, .(Name, extTrackingID, Department)]
#   df11<-distinct(df11)
#   df12<-merge(df10,df11, by=c('extTrackingID','Department'),all=T)
#   df12$Name[is.na(df12$Name)]<-' '
#   df12$Answered<-ifelse(df12$Answered>0,1,0)
#   df12$Inter<-0
#   df_report_final2<-rbind(df7,df12)
#   df_report_final2<-rbind(df_report_final2,c6[,c(2,1,3,4,6,7)])
#   a<-df[df$Direction=="Outbound"&df$Answered==1&df$Department==""&duplicated(df$extTrackingID),]
#   b<-df[df$extTrackingID %in% a$extTrackingID & Direction=="Inbound" & Department != "",.(sum=sum(Answered)),extTrackingID]
#   c<-b[b$sum==0,]
#   c<-c[!c$extTrackingID %in% voice]
#   d<-df[df$extTrackingID%in%c$extTrackingID,]
#   
#   #Sales, Admin, Service, Parts. Warranty
#   e<-d[d$Direction=='Outbound'&d$Answered==1&d$Department=="",]
#   f<-e[e$Caller.ID%like%"Sales"|e$Caller.ID%like%"Admin"|e$Caller.ID%like%"Service"|e$Caller.ID%like%"Parts"|e$Caller.ID%like%"Warrenty",]
#   f$Department[f$Caller.ID%like%"Sales"]<-"Sales"
#   f$Department[f$Caller.ID%like%"Admin"]<-"Admin"
#   f$Department[f$Caller.ID%like%"Service"]<-"Service"
#   f$Department[f$Caller.ID%like%"Parts"]<-"Parts"
#   f$Department[f$Caller.ID%like%"Warrenty"]<-"Warrenty"
#   f$Department<-paste(f$Department," (", f$Group, ")",sep="")
#   f<-f[!duplicated(f$extTrackingID),]
#   f<-f[,.(Department,Caller.ID,extTrackingID,DateStamp, Answered)]
#   
#   f$Inter<-ifelse(f$extTrackingID %in%df_report_final2$extTrackingID[df_report_final2$Inter==1], 1,0)
#   
#   
#   #f<-e[e$Caller.ID=="HG ALL" | e$Caller.ID=="",]
#   #liste<-e$extTrackingID[!e$extTrackingID%in%f$extTrackingID & !e$LocalID%in%f$LocalID]
#   df_report_final2$Answered[paste(df_report_final2$Department,df_report_final2$extTrackingID)%in% paste(f$Department,f$extTrackingID)]<-1
#   df_report_final2<-merge(df_report_final2, f, by=c("extTrackingID", "Department"),all=T)
#   df_report_final2$Caller.ID<-ifelse(is.na(df_report_final2$Caller.ID), df_report_final2$Name, df_report_final2$Caller.ID)
#   df_report_final2$DateStamp.x<-ifelse(!is.na(df_report_final2$DateStamp.y),df_report_final2$DateStamp.y,df_report_final2$DateStamp.x)
#   df_report_final2$Answered.x<-ifelse(!is.na(df_report_final2$Answered.y),df_report_final2$Answered.y,df_report_final2$Answered.x)
#   df_report_final2$Inter.x<-ifelse(!is.na(df_report_final2$Inter.y),df_report_final2$Inter.y,df_report_final2$Inter.x)
#   df_report_final2$Name<-NULL
#   df_report_final2$DateStamp.y<-NULL
#   df_report_final2$Answered.y<-NULL
#   df_report_final2$Inter.y<-NULL
#   colnames(df_report_final2)<-c("extTrackingID", "Department","DateStamp","Answered","Inter", "Name")
#   
#   g<-e[!e$extTrackingID%in%f$extTrackingID,]
#   h<-df_report_final2[df_report_final2$extTrackingID%in%g$extTrackingID,]
#   h$Department<-"Unknown"
#   h$Name<-"Unknown"
#   h$Answered<-1
#   h<-h[!duplicated(h$extTrackingID)]
#   
#   df_report_final2<-rbind(df_report_final2,h)
#   df_report_final2$Name[is.na(df_report_final2$Name)]<-" "
#   write.csv(df_report_final2,paste(i,".csv"))
# }


#for (i in 1:length(file_list)){
#  clean2(file_list[i],i)
#}

#file_list1<-list.files(path = "/Users/xiaotian/Desktop/project/df2", pattern = "*.csv$",recursive = TRUE,full.names = TRUE)
#final2<-ldply(file_list1, read.csv, header=TRUE)


#a<-final2[final2$Name=="Voice Portal",]
#b<-final2[final2$extTrackingID%in%a$extTrackingID,]
#c<-b[b$Name!="Voice Portal" & b$Answered == 1,]

final1=read.csv("df_report_final1.csv")
final2=read.csv("df_report_final2.csv")
#final3<-read.csv('df2.csv')

final2$X=NULL
final2$DateStamp=as.POSIXlt(final2$DateStamp,format = "%m/%d/%Y/%H")
final2$weekday= weekdays(final2$DateStamp)
final2$time = format(final2$DateStamp,"%H")       
final2$year = format(final2$DateStamp,"%Y")
final2$month = format(final2$DateStamp,"%m")
final2$date = format(final2$DateStamp,"%d")
final2$region = gsub("\\D+", "", final2$Department)
final2$department=gsub("\\d+|\\(|\\)|\\s", "", final2$Department)
final2$Department=NULL
final2$DateStamp=NULL
final2=final2[,c(1,7,8,9,6,5,10,11,2,3,4)]
write.csv(final2[1:1000000,],"df2_1.csv")
write.csv(final2[1000001:1709146,],"df2_2.csv")

final1$X=NULL
final1$DateStamp=as.POSIXlt(final1$DateStamp,format = "%m/%d/%Y/%H")
final1$weekday= weekdays(final1$DateStamp)
final1$time = format(final1$DateStamp,"%H")       
final1$year = format(final1$DateStamp,"%Y")
final1$month = format(final1$DateStamp,"%m")
final1$date = format(final1$DateStamp,"%d")
final1=final1[,c(1,2,7,8,9,6,5,3)]
write.csv(final1,"df1.csv")

#df2<-data.table(final2)
#a<-df2[,length(extTrackingID),.(Name=Name, Department=department)]


#df<-read.csv("/Users/xiaotian/Desktop/LandPro Data/TPX Weekly Reports/01-28-2021.csv")

#df2<-df2[df2$Name%like%"Sales"|df2$Name%like%"Admin"|df2$Name%like%"Service"|df2$Name%like%"Parts"|df2$Name%like%"Warrenty",]
#c<-rbind(df2[df2$Name%like%"Sales" & !df2$department %like% "Sales",],
#df2[df2$Name%like%"Admin" & !df2$department %like% "Admin",],
#df2[df2$Name%like%"Service" & !df2$department %like% "Service",],
#df2[df2$Name%like%"Parts" & !df2$department %like% "Parts",],
#df2[df2$Name%like%"Warrenty" & !df2$department %like% "Warrenty",])

#differentnames<-unique(c$Name)

#d<-final2[,c(1,8)]
#sum(duplicated(d))

#p<-p[p$Name!="Voice Portal",]
#c<-rbind(df2[df2$Name%like%"Sales" & !df2$department %like% "Sales",],
#df2[df2$Name%like%"Admin" & !df2$department %like% "Admin",],
#df2[df2$Name%like%"Service" & !df2$department %like% "Service",],
#df2[df2$Name%like%"Parts" & !df2$department %like% "Parts",],
#df2[df2$Name%like%"Warrenty" & !df2$department %like% "Warrenty",])