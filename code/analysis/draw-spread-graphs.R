degree=read.csv(file='output-degree',header=FALSE, sep=',')
degreediscount=read.csv(file='output-degreediscount', header=FALSE,sep=',')
pagerank=read.csv(file='output-pagerank', header=FALSE,sep=',')
random=read.csv(file='output-random', header=FALSE,sep=',')

celf=read.csv(file='output-celf', header=FALSE,sep=',')
edgesampling=read.csv(file='output-edgesampling', header=FALSE,sep=',')
multicycle=read.csv(file='output-multicycle', header=FALSE,sep=',')
singlecycle=read.csv(file='output-singlecycle', header=FALSE,sep=',')

plot(sort(degree[,2]) ,xlab='Seed Size', ylab='Total Spread', lty=1, type="l", ylim = c(0,230))
lines(sort(degreediscount[,2]) , lty=2,type="l")
lines(sort(pagerank[,2]) , lty=3,type="l")
lines(sort(random[,2]),  lty=4,type="l")
legend('topleft',c("degree","degreediscount","pagerank","random"), lty =c(1,2,3,4),cex=0.7,bty = "n")
title("Hep Graph (Heuristics methods)")

plot(sort(celf[,2]) , lty=1, xlab='Seed Size', ylab='Total Spread',type="l",ylim = c(0,230))
lines(sort(edgesampling[,2]) , lty=2)
lines(sort(multicycle[,2]) , lty=3)
lines(sort(singlecycle[,2]) , lty=4)
legend('topleft',c("greedy","edgesampling","multicycle","singlecycle"), lty =c(1,2,3,4),cex=0.7,bty = "n")

title("Hep Graph (Search methods)")

