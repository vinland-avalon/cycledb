#! /bin/sh
 
TAG_VALUE_NUM=4
TAG_KEY_NUM=3

SERIES_KEY_GENERATOR="full_permutation_generator"
# SERIES_KEY_GENERATOR="random_generator"

# to test how tag_value_num will affect porformance
# [4,11)
while [ "$TAG_VALUE_NUM" -lt 11 ]; do
    make benchmark_params TAG_KEY_NUM=$TAG_KEY_NUM TAG_VALUE_NUM=$TAG_VALUE_NUM SERIES_KEY_GENERATOR=$SERIES_KEY_GENERATOR
    TAG_VALUE_NUM=$(($TAG_VALUE_NUM+1))
done

TAG_VALUE_NUM=4
TAG_KEY_NUM=3

# to test how tag_key_num will affect porformance
# [3,6)
while [ "$TAG_KEY_NUM" -lt 6 ]; do
    make benchmark_params TAG_KEY_NUM=$TAG_KEY_NUM TAG_VALUE_NUM=$TAG_VALUE_NUM SERIES_KEY_GENERATOR=$SERIES_KEY_GENERATOR
    TAG_KEY_NUM=$(($TAG_KEY_NUM+1))
done

# #! /bin/sh
 
# TAG_VALUE_NUM=10
# TAG_KEY_NUM=5

# SERIES_KEY_GENERATOR="diagonal_generator"

# # to test how tag_value_num will affect porformance
# # [10,51), step: 10
# while [ "$TAG_VALUE_NUM" -lt 51 ]; do
#     make benchmark_params TAG_KEY_NUM=$TAG_KEY_NUM TAG_VALUE_NUM=$TAG_VALUE_NUM SERIES_KEY_GENERATOR=$SERIES_KEY_GENERATOR
#     TAG_VALUE_NUM=$(($TAG_VALUE_NUM+10))
# done

# TAG_VALUE_NUM=10
# TAG_KEY_NUM=5

# # to test how tag_key_num will affect porformance
# # [5,16), step: 3
# while [ "$TAG_KEY_NUM" -lt 16 ]; do
#     make benchmark_params TAG_KEY_NUM=$TAG_KEY_NUM TAG_VALUE_NUM=$TAG_VALUE_NUM SERIES_KEY_GENERATOR=$SERIES_KEY_GENERATOR
#     TAG_KEY_NUM=$(($TAG_KEY_NUM+3))
# done