# create vector
vec1 = c(1:20)

# remove 3rd element
vec1 <- vec1[-3]

# add 2 to remaining elements
vec1 <- vec1 + 2

# multply vector bt 5
vec1 <- vec1 * 5

# take sqrt for each elem
vec1 <- sqrt(vec1)

# take natural log per elem
vec1 <- log(vec1)


# create 3x3 matrix
mat <- matrix(c(1,1,1, 2,2,2 ,3,3,3), nrow = 3, ncol = 3)
print(mat)