# map-reduce in python
#     - map
#     - filter
#     - reduce

# - finding sum of squares of all even numbers in a given range

# traditional way of coding
from functools import reduce


sum = 0
numRange = range(1, 100)

for i in numRange:
    if i%2==0:
        sum+=(i*i)

print(sum)

#map-reduce way of doing the same thing
# finding square of each element can be done with help of map
# if condition can be achieved using filter
# calculating sum (or any type of aggregation) can be done using reduce

# doing filter first instead of map will be more optimmised

filter_even = list(filter(lambda x: x%2==0, numRange))
map_squares = list(map(lambda x: x*x, filter_even))
sum_of_squares_of_even = reduce(lambda total, element: total+element,  map_squares, 0)
print(sum_of_squares_of_even)