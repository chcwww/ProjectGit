start <- proc.time()
dataTran <- read.csv('C://Users//chcww//Downloads//newdata.csv',header=TRUE)
dataOffer <- read.csv('C://Users//chcww//Downloads//offers.csv',header=TRUE)
dataHist <- read.csv('C://Users//chcww//Downloads//trainhistory.csv',header=TRUE)
data1 <- read.csv('C://Users//chcww//Downloads//testhistory.csv',header=TRUE)
feature <- read.csv('C://Users//chcww//Downloads//featureNew.csv')

dataHist <- dataHist[, c(-5, -6)]
dataHist <- rbind(dataHist, data1)
str(dataNHist)

tranId <- table(dataTran$id)
navigate <- names(tranId) %in% dataHist$id
betterId <- tranId[navigate]
newCheck <- dataTran$id %in% names(betterId)
dataTRHist <- dataTran[dataTran$id %in% names(betterId),]

str(dataTran)

freq <- table(dataTRHist$id)
HistId <- table(dataHist$id)
navigate1 <- names(HistId) %in% names(freq)
betterId1 <- HistId[navigate1]
newCheck <- dataHist$id %in% names(betterId1)
dataNHist <- dataHist[dataHist$id %in% names(betterId1),]

dataNHist$freq <- freq

newf <- dataNHist$freq
i = 1
amount <- NULL

for(k in 1:length(newf)) {
  times <- newf[k]
  amount[k] <- sum(dataTRHist$purchaseamount[i:(i+times-1)])
  i = i+times
} 

dataTran[dataTran$purchaseamount<0,]
length(which(dataTran[, 'purchaseamount'] < 0))/length(dataTran$id)


dataNHist$amount <- amount

dataNHist$date <- as.POSIXct(dataNHist$offerdate)
now <- as.POSIXct(Sys.Date())
#now - dataNHist$date[1]
dataNHist$date1 <- now - dataNHist$date
#dataNHist$date1[1] > dataNHist$date1[2]

dataTRHist$date1 <- now - as.POSIXct(dataTRHist$date)


newd <- dataTRHist$date1
i = 1
datee <- NULL
# start <- proc.time()
for(k in 1:length(newf)) {
  times <- newf[k]
  datee[k] <- min(newd[i:(i+times-1)])
  i = i+times
} 

dataNHist$recency <- datee


dataNHist$dateBeforOffer <- dataNHist$recency - dataNHist$date1

write.table(dataNHist, file = "RFMdata.csv", sep = ",", row.names = FALSE)

end <- proc.time()
runningTime <- end - start
print(runningTime)



dataKmeans <- 
  read.csv('C://R1091//new//RFMdata.csv',header=TRUE)

dataKmeans$stR <- scale(dataKmeans["dateBeforOffer"])
dataKmeans$stF <- scale(dataKmeans["freq"])
dataKmeans$stM <- scale(dataKmeans["amount"])


library(rpart)
tHist <- rpart(repeater~stR+stF+stM, data = dataKmeans, method = "class",
               control = rpart.control(minsplit = 10,cp = 0.001,maxdeptth = 30))
#t
#plot(t,compress=T,margin=0.2)#compress引數指定以更稠密的方式繪製決策樹
#text(t,cex=0.5)
#prp(t,type=3,digits=3)
png("desition_tree_History.png", width = 1024, height = 768)
fancyRpartPlot(tHist, sub = "History relationship", cex = 1, digits=4)
dev.off()










# rm(list = ls())


cateProb <- xtabs(~category, data = dataTran)
prop.table(cateProb)

cateProb2 <- xtabs(~repeater, data = dataHist)
prop.table(cateProb2)

cateProb3 <- xtabs(~repeattrips, data = dataHist)
prop.table(cateProb3)

dataHist$repscore <- NULL;
dataHist$repscore[dataHist$repeattrips > 0] <- 1
dataHist$repscore[dataHist$repeattrips > 5] <- 2
dataHist$repscore[dataHist$repeattrips > 10] <- 3
dataHist$repscore[dataHist$repeattrips > 15] <- 4
dataHist$repscore[dataHist$repeattrips > 20] <- 5
dataHist$repscore[dataHist$repeattrips == 0] <- 0

cateProb4 <- xtabs(~repscore, data = dataHist)
prop.table(cateProb4)




newHist <- dataHist[dataHist$repeattrips != 0,]
cateProbnew <- xtabs(~repscore, data = newHist)
prop.table(cateProbnew)


goodconsumer <- newHist$id[newHist$repscore == 5]


check <- dataTran$id==goodconsumer[1]
for(person in goodconsumer) {
  check1 <- dataTran$id==person
  check = check | check1
}

goodTran <- dataTran[check,]

buyAmount <- NULL
i <- 1
for(person in goodconsumer) {
  check1 <- dataTran$id==person
  buyAmount[i] <- sum(dataTran$purchaseamount[check1])
  i = i+1
}

buySoMuch <- goodconsumer[buyAmount<100]
for(person in buySoMuch) {
  print(as.character(person))
  print(dataHist$repeattrips[dataHist$id == person])
}

library(rpart)
library(rattle)

dataHist$rep[dataHist$repeater == "t"] <- 1
dataHist$rep[dataHist$repeater == "f"] <- 0


changeMoney <- function(id){
  dataHist$consumePrice[dataHist$id == id] <- 
    sum(dataTran$purchaseamount[dataTran$id == id])
}
N <- matrix(dataHist$id, ncol = 1)
start <- proc.time()
# apply(N, 1, changeMoney)
changeMoney(dataHist[1])
end <- proc.time()
runningTime <- end - start
print(runningTime)

dataHist$offerS <- as.character(dataHist$offer)
dataHist$chainS <- as.character(dataHist$chain)
dataHist$marketS <- as.character(dataHist$market)
dataHist$year <- substr(dataHist$offerdate, 1, 4)
dataHist$month <- substr(dataHist$offerdate, 6, 7)
dataHist$day <- substr(dataHist$offerdate, 9, 10)

tHist <- rpart(repeater~rep, data = dataHist, 
               method = "class", parms = list(split = "gini"))
#t
#plot(t,compress=T,margin=0.2)#compress引數指定以更稠密的方式繪製決策樹
#text(t,cex=0.5)
#prp(t,type=3,digits=3)
png("desition_tree_History.png", width = 1024, height = 768)
fancyRpartPlot(tHist, sub = "History relationship", cex = 1, digits=4)
dev.off()











