#! /bin/sh
 
TAG_VALUE_NUM=4
TAG_KEY_NUM=3

# to test how tag_value_num will affect porformance
# [4,11)
while [ "$TAG_VALUE_NUM" -lt 11 ]; do
    make benchmark_params TAG_KEY_NUM=$TAG_KEY_NUM TAG_VALUE_NUM=$TAG_VALUE_NUM
    TAG_VALUE_NUM=$(($TAG_VALUE_NUM+1))
done

TAG_VALUE_NUM=4
TAG_KEY_NUM=3

# to test how tag_key_num will affect porformance
# [3,6)
while [ "$TAG_KEY_NUM" -lt 6 ]; do
    make benchmark_params TAG_KEY_NUM=$TAG_KEY_NUM TAG_VALUE_NUM=$TAG_VALUE_NUM
    TAG_KEY_NUM=$(($TAG_KEY_NUM+1))
done