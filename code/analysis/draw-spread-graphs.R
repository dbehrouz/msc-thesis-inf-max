degree=read.csv(file='output-degree',header=FALSE, sep=',')
degreediscount=read.csv(file='output-degreediscount', header=FALSE,sep=',')
pagerank=read.csv(file='output-pagerank', header=FALSE,sep=',')
random=read.csv(file='output-random', header=FALSE,sep=',')

edgesampling=read.csv(file='output-edgesampling', header=FALSE,sep=',')
multicycle=read.csv(file='output-multicycle', header=FALSE,sep=',')
singlecycle=read.csv(file='output-singlecycle', header=FALSE,sep=',')


plot(degree ,xlab='Seed Size', ylab='Total Spread', pch=1)
points(degreediscount , pch=2)
points(pagerank , pch=3)
points(random,  pch=4)

legend(5,95,c("edgesampling","degree","degreediscount","pagerank","random","multicycle","singlecycle"), lty =c(1,2,3,4,5,6,24),cex=0.7)


points(multicycle , pch=6)
points(singlecycle , pch=7)





