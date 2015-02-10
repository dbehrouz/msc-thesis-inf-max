degree=read.csv(file='degree',header=FALSE, sep=':')
degreediscount=read.csv(file='degreediscount', header=FALSE,sep=':')
edgesampling=read.csv(file='edgesampling', header=FALSE,sep=':')
pagerank=read.csv(file='pagerank', header=FALSE,sep=':')
random=read.csv(file='random', header=FALSE,sep=':')

degree[2]=degree[2]/1000
degreediscount[2]=degreediscount[2]/1000
edgesampling[2]=edgesampling[2]/1000
pagerank[2]=pagerank[2]/1000
random[2]=random[2]/1000

plot(edgesampling, type="l" ,xlab='Seed Size', ylab='Time (s)', lty=1, ylim=c(0,100))
lines(degree, type="l" , lty=2)
lines(degreediscount, type="l" , lty=3)
lines(pagerank, type="l" , lty=4)
lines(random, type="l" , lty=5)