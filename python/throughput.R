producers <- c(1, 2, 3, 4, 5)
throughput <- c(16, 17.5, 18.5, 19, 21)

# Graph cars using blue points overlayed by a line 
plot(producers, throughput, type="o", col="blue", ann=FALSE)

# Create a title with a red, bold/italic font
title(main="Batch Throughput")

title(xlab="Number of Producers")
title(ylab="Number of Throughput Batches ")
