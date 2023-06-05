# install.packages("ROCR")
library(ROCR)

.empRocInfo <- function(scores, classes) {
  # This software comes with absolutely no warranty. Use at your own risk.
  #
  # Provides information related to the ROC given probability and class label vectors. 
  # This function is not to be called directly in a normal use case. 
  # Instead, the other functions in this package call this function when necessary.
  #
  #
  # Arguments:
  #   scores: A vector of probability scores.
  #   classes: A vector of true class labels.
  # Value:
  #   A RocInfo object with six components:
  #     n0: Number of positive observations.
  #     n1: Number of negative observations.
  #     pi0: Prior probability of positive observation.
  #     pi1: Prior probability of negative observation.
  #     F0: Convex hull of ROC y values.
  #     F1: Convex hull of ROC x values.
  if (length(scores) != length(classes)) {
    stop('Length of scores and classes vectors is not equal')
  }
  prediction <- prediction(scores, classes)
  perf <- performance(prediction, "rch")
  n0 <- prediction@n.pos[[1]]
  n1 <- prediction@n.neg[[1]]
  pi0 <- n0 / (n0 + n1)
  pi1 <- n1 / (n0 + n1)
  F0 <- perf@y.values[[1]]
  F1 <- perf@x.values[[1]]
  list(n0=n0, n1=n1, pi0=pi0, pi1=pi1, F0=F0, F1=F1)
}



empChurn <- function(scores, classes, alpha = 6, beta = 14, clv = 200, d = 10, f = 1) {
  # This software comes with absolutely no warranty. Use at your own risk.
  #
  # Adapted from:
  # Verbraken, T., Wouter, V. and Baesens, B. (2013). A Novel Profit Maximizing 
  # Metric for Measuring Classification Performance of Customer Churn Prediction
  # Models. Knowledge and Data Engineering, IEEE Transactions on. 25 (5): 
  # 961-973.
  # Available Online: http://ieeexplore.ieee.org/iel5/69/6486492/06165289.pdf
  #
  # Estimates the EMP for customer churn prediction, considering constant CLV
  # and a given cost of contact f and retention offer d.
  #
  #
  # Arguments:
  #   scores: A vector of probability scores.
  #   classes: A vector of true class labels.
  #   p0: Percentage of cases on the first point mass of the LGD distribution 
  #   (complete recovery).
  #   p1: Percentage of cases on the second point mass of the LGD distribution 
  #   (complete loss).
  #   ROI: Constant ROI per granted loan. A percentage.
  # Value:
  #   An EMP object with two components.
  #     EMP: The Expected Maximum Profit of the ROC curve at EMPfrac cutoff.}
  #     EMPfrac: The percentage of cases that should be excluded, that is, 
  #     the percentual cutoff at EMP profit.  
  
  roc = .empRocInfo(scores, classes)
  
  egamma <- alpha / (alpha+beta);
  delta  <- d/clv
  phi    <- f/clv
  
  B <- function(x,a,b){ pbeta(x,a,b)*beta(a,b) } # incomplete beta function
  
  gamma <- c(0,(roc$pi1*(delta+phi)*diff(roc$F1)+roc$pi0*phi*diff(roc$F0))/(roc$pi0*(1-delta)*diff(roc$F0)))
  gamma <- c(gamma[gamma<1],1)
  
  indE <- max(which((gamma<egamma)==T))
  MP = clv*((egamma*(1-delta)-phi)*roc$pi0*roc$F0[indE]-(delta+phi)*roc$pi1*roc$F1[indE]);
  MPfrac = roc$pi0*roc$F0[indE] + roc$pi1*roc$F1[indE];
  
  gammaii <- head(gamma, n=-1)
  gammaie <- tail(gamma, n=-1)
  F0 <- roc$F0[1:length(gammaii)]
  F1 <- roc$F1[1:length(gammaii)]
  
  contr0 <- ( clv*(1-delta)*roc$pi0*F0)                       * (B(gammaie,alpha+1,beta)-B(gammaii,alpha+1,beta))/B(1,alpha,beta)
  contr1 <- (-clv*(phi*roc$pi0*F0 + (delta+phi)*roc$pi1*F1))  * (B(gammaie,alpha  ,beta)-B(gammaii,alpha,  beta))/B(1,alpha,beta)
  EMP <- sum(contr0+contr1)
  EMPfrac <- c(t(((B(gammaie,alpha,beta)-B(gammaii,alpha,beta))/B(1,alpha,beta))) %*% (roc$pi0*F0+roc$pi1*F1))
  
  list(MP=MP, MPfrac=MPfrac, EMP=EMP, EMPfrac=EMPfrac)
}


preData <- read.csv("C://Users//chcww//Downloads//cvEncoder_pred_xgb.csv")
aucData <- read.csv("C://Users//chcww//Downloads//cvEncoder_auc_f1_xgb.csv")


preData <- read.csv("C://Users//chcww//Downloads//onehot_std_smenn_logistic_pred.csv")
aucData <- read.csv("C://Users//chcww//Downloads//cvResample_auc_f1_DecisionTree.csv")


preData


empChurn(preData[1], preData[2], clv = 4800)$EMP

y_test <- preData['repeater']
pred <- preData['none_none']
colnames(preData)
# acc <- mean(pred == y_test)

EMPC <- NULL
for (i in colnames(preData)[-length(colnames(preData))]) {
  pred <- preData[i]
  EMPC <- c(EMPC, empChurn(pred, y_test, clv = 4800)$EMP)
}
plot(EMPC)

ds <- cbind(aucData, EMPC)
write.csv(ds, "C://Users//chcww//Downloads//EMPCResample_DT.csv")




name <- c('Logistic', 'MLP', 'SVM', 'Decision Tree', 'XGBoost', 'KNN')
auc <- c(0.710656, 0.6977478, 0.7115313, 0.6095668, 0.6897046, 0.6626271)
f1 <- c(381.7312, 381.7152, 381.7369, 381.7152, 381.7294, 381.7132)
EMPC <- c(0.4873259, 0.4782265, 0.3866467, 0.4634007, 0.3498411, 0.476403)

allData <- data.frame(name, auc, f1, EMPC)
write.csv(allData, "C://Users//chcww//Downloads//EMPCModel.csv")


EMPCEncode <- read.csv("C://Users//chcww//Downloads//project//EMPCOriginal_xgb.csv")


a <- EMPCEncode$auc
EMPCEncode['sa'] <- (a - mean(a)) / sd(a)
b <- EMPCEncode$auc
EMPCEncode['sb'] <- (b - mean(b)) / sd(b)
EMPCEncode['check'] <- EMPCEncode['sa'] + EMPCEncode['sb']
cc <- which.max(EMPCEncode$check)

cc <- which.max(rank(EMPCEncode$auc) + rank(EMPCEncode$EMPC))


tk <- cbind(EMPCEncode[cc, ][[3]], EMPCEncode[cc, ][[5]])

tk <- paste0("(#", round(tk[1], 2), ", #", round(tk[2], 2), ")")

# EMPCEncode['tot'] = ""
# EMPCEncode[aa, 'tot'] = tk

enc <- NULL
nor <- NULL
i<-1
for (i in 1:length(EMPCEncode[[1]])) {
  a <- as.data.frame(strsplit(EMPCEncode$name, "_"))[[i]]
  enc <- c(enc, a[1])
  nor <- c(nor, a[2])
}
EMPCEncode_new <- cbind(enc, nor, EMPCEncode[, c(2, 3, 4, 5)])
colnames(EMPCEncode_new) <- c('encode', 'st', 'name', 'auc', 'f1', 'empc')


library(ggplot2)
library(ggrepel)

aa <- which.max(EMPCEncode_new$auc)
bb <- which.max(EMPCEncode_new$empc)


k1 <- cbind(EMPCEncode_new[aa, ][[4]], EMPCEncode_new[aa, ][[6]])
k2 <- cbind(EMPCEncode_new[bb, ][[4]], EMPCEncode_new[bb, ][[6]])

t1 <- paste0("(*", round(k1[1], 2), ", ", round(k1[2], 2), ")")
t2 <- paste0("(", round(k2[1], 2), ", *", round(k2[2], 2), ")")

EMPCEncode_new['max'] = ""
EMPCEncode_new[aa, 'max'] = t1
EMPCEncode_new[bb, 'max'] = t2
EMPCEncode_new[cc, 'max'] = tk


shapes = c(6:14, 17, 18)[-5]


gg <- ggplot(EMPCEncode_new, aes(x=auc, y=empc, size = f1, label=name, col=st)) +
  geom_point(aes(shape = encode), alpha=0.7) + 
  scale_shape_manual(values=shapes) +
  ggtitle("AUC v.s. EMPC for combination of preprocessing        model: SVM") +
  xlab("AUC score") +
  ylab("EMPC score") 
# +
#   geom_text_repel(aes(auc, empc,
#                       label=max),
#                   size=5,point.padding = NA, force = 1, max.overlaps = 20
#                   # box.padding=unit(0.5, "lines")
#   )
library(plotly)

ggplotly(gg) %>% config(scrollZoom=TRUE) %>% layout(
  dragmode="pan", font=list(family="consolas"))


# prediction <- prediction(pred, y_test)
# perf <- performance(prediction, "tpr", "fpr")
# perf
# 
# library(ggplot2)
# 
# g <- ggplot(chic, aes(x = y_test, y = temp))







# # 加载R包，没有安装请先安装  install.packages("包名") 
# library(pROC)
# library(ggplot2)
# 
# # 读取ROC数据文件
# 
# # ROC计算
# pp <- data.frame("y" = y_test, "p" = pred)
# colnames(pp) <- c("y", "p")
# rocobj <- roc(pp, y, p,
#               # controls=df[,2][df[,1]=="Good"],  # 可以设置实验组或对照组
#               # cases=df[,2][df[,1]=="Poor"],
#               smooth = F       # 曲线是否光滑，当光滑时，无法计算置信区间
# ) 
# # 计算临界点/阈值
# cutOffPoint <- coords(rocobj, "best")
# cutOffPointText <- paste0(round(cutOffPoint[1],3),"(",round(cutOffPoint[2],3),",",round(cutOffPoint[3],3),")")
# 
# # 计算AUC值
# auc<-auc(rocobj)[1]
# # AUC的置信区间
# auc_low<-ci(rocobj,of="auc")[1]
# auc_high<-ci(rocobj,of="auc")[3]
# 
# # 计算置信区间
# ciobj <- ci.se(rocobj,specificities=seq(0, 1, 0.01))
# data_ci<-ciobj[1:101,1:3]
# data_ci<-as.data.frame(data_ci)
# x=as.numeric(rownames(data_ci))
# data_ci<-data.frame(x,data_ci)
# 
# # 绘图
# ggroc(rocobj,
#       color="red",
#       size=1,
#       legacy.axes = F # FALSE时 横坐标为1-0 specificity；TRUE时 横坐标为0-1 1-specificity
# )+
#   theme_classic()+
#   geom_segment(aes(x = 1, y = 0, xend = 0, yend = 1),        # 绘制对角线
#                colour='grey',
#                linetype = 'dotdash'
#   ) +
#   geom_ribbon(data = data_ci,                                # 绘制置信区间
#               aes(x=x,ymin=X2.5.,ymax=X97.5.),               # 当legacy.axes=TRUE时， 把x=x改为x=1-x
#               fill = 'lightblue',
#               alpha=0.5)+
#   geom_point(aes(x = cutOffPoint[[2]],y = cutOffPoint[[3]]))+ # 绘制临界点/阈值
#   geom_text(aes(x = cutOffPoint[[2]],y = cutOffPoint[[3]],label=cutOffPointText),vjust=-1) # 添加临界点/阈值文字标签


final <- NULL
models <- c("DT", "knn", "logistic", "MLP", "svm", "xgb")

for (model in models){ 
  name <- paste0("C://Users//chcww//Downloads//project//EMPCResample_", model, ".csv")
  catch <- read.csv(name)
  catch$model <- model
  final <- rbind(final, catch)
}
write.csv(final[, -1], "C://Users//chcww//Downloads//EMPCResample.csv")


final <- NULL
models <- c("DT", "knn", "logistic", "MLP", "svm", "xgb")

for (model in models){ 
  name <- paste0("C://Users//chcww//Downloads//project//EMPCOriginal_", model, ".csv")
  catch <- read.csv(name)
  catch$model <- model
  final <- rbind(final, catch)
}
write.csv(final[, -1], "C://Users//chcww//Downloads//EMPCOriginal.csv")
