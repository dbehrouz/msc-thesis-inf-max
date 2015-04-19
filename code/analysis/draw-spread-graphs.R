degree=read.csv(file='output-degree',header=FALSE, sep=',')
degreediscount=read.csv(file='output-degreediscount', header=FALSE,sep=',')
pagerank=read.csv(file='output-pagerank', header=FALSE,sep=',')
random=read.csv(file='output-random', header=FALSE,sep=',')

celf=read.csv(file='output-celf', header=FALSE,sep=',')
edgesampling=read.csv(file='output-edgesampling', header=FALSE,sep=',')
multicycle=read.csv(file='output-multicycle', header=FALSE,sep=',')
singlecycle=read.csv(file='output-singlecycle', header=FALSE,sep=',')

plot(sort(degree[,2]) ,xlab='Seed Size', ylab='Total Spread', lwd= 3, lty=1, type="l", ylim = c(0,230))
lines(sort(degreediscount[,2]) , lwd= 3,lty=2,type="l")
lines(sort(pagerank[,2]) , lwd= 3,lty=3,type="l")
lines(sort(random[,2]),  lwd= 3,lty=4,type="l")
legend('topleft',c("degree","degreediscount","pagerank","random"), lwd = 3,lty =c(1,2,3,4),cex=1.0,bty = "n")
title("Hep Graph (Heuristics methods)")

plot(sort(celf[,2]) , lty=1, xlab='Seed Size', ylab='Total Spread',lwd= 2,type="l",ylim = c(0,230))
lines(sort(edgesampling[,2]) ,lwd= 2, lty=2)
lines(sort(multicycle[,2]) ,lwd= 2, lty=3)
lines(sort(singlecycle[,2]) ,lwd= 2, lty=4)
legend('topleft',c("greedy","edgesampling","multicycle","singlecycle"), lwd=3,lty =c(1,2,3,4),cex=1.0,bty = "n")

title("Hep Graph (Search methods)")

