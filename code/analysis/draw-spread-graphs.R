degree=read.csv(file='output-degree',header=FALSE, sep=',')
#degreediscount=read.csv(file='output-degreediscount', header=FALSE,sep=',')
pagerank=read.csv(file='output-pagerank', header=FALSE,sep=',')
random=read.csv(file='output-random', header=FALSE,sep=',')

edgesampling=read.csv(file='output-edgesampling', header=FALSE,sep=',')
multicycle=read.csv(file='output-multicycle', header=FALSE,sep=',')
singlecycle=read.csv(file='output-singlecycle', header=FALSE,sep=',')


plot(sort(degree[,2]) ,xlab='Seed Size', ylab='Total Spread', lty=1, type="l")
lines(sort(edgesampling[,2]) , lty=2,type="l")
#points(degreediscount , pch=2)
lines(sort(pagerank[,2]) , lty=3,type="l")
lines(sort(random[,2]),  lty=4,type="l")

legend(5,95,c("degree","edgesampling","pagerank","random","multicycle","singlecycle"), lty =c(1,2,3,4,6,7),cex=0.7)


lines(sort(multicycle[,2]) , lty=6,type="l")
lines(sort(singlecycle[,2]) , lty=7,type="l")





