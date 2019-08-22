#!/bin/bash

set -eo pipefail
# Set env var ${NL} because "\n" can not be converted to new line for unknown escaping reason
export NL="
"

# Retrieve cell content from $RAW_STR according to the row and column numbers,
# and set it to the given variable name.
# Arg 1 (output): Variable name.
# Arg 2 (input): Row number.
# Arg 3 (input): Column number.
# Example:
# 1: @out_sh 'get_cell USER_NAME 3 2': SELECT id,name,status FROM users;
# Assume 'SELECT id,name,status FROM users;' returns:
# | id | name  | status |
# |----+-------+--------|
# | 1  | Jonh  | Alive  |
# | 2  | Alice | Dead   |
# by calling `get_cell USER_NAME 2 3' will set $USER_NAME to 'Jonh'.
get_cell() {
    var_name=$1
    row=$2
    col=$3
    cmd="echo \"\$RAW_STR\" | awk -F '|' 'NR==$row {print \$$col}' | awk '{\$1=\$1;print}'"
    output=`eval $cmd`
    eval $var_name="$output"
}

# Behaves similar to the get_cell(), except $RETRIEVE_TOKEN will be set to the cell value.
# Arg 1 (output): Variable name.
# Arg 2 (input): Row number.
# Arg 3 (input): Column number.
get_token_cell() {
    var_name=$1
    get_cell $@
    eval RETRIEVE_TOKEN="\$$var_name"
}

# Generate $MATCHSUBS and echo the $RAWSTR based on the given original string and replacement pairs.
# Arg 1n (input): The original string to be replaced.
# Arg 2n (input): The replacement string.
# Example:
# 1: @out_sh 'match_sub $USER_NAME user1, $USER_ID, id1': SELECT id,name,status FROM users;
# here we assume $USER_NAME and $USER_ID has been set to 'Jonh' and '1' already. Then the above
# statement will generate $MATCHSUBS section:
# m/\bJonh\b/
# s/\bJonh\b/user1/
# \b here is for matching the whole word. (word boundaries)
match_sub() {
    to_replace=""
    for var in "$@"
        do
        if [ -z "$to_replace" ]
        then
            to_replace=$var
        else
            # \b is trying to match the whole word to make it more stable.
            export MATCHSUBS="${MATCHSUBS}${NL}m/\\b${var}\\b/${NL}s/\\b${var}\\b/${to_replace}/${NL}"
            to_replace=""
        fi
    done
    echo "${RAW_STR}"
}

# Generate $MATCHSUBS and Trim the Tailing spaces.
# This is similar to match_sub() but dealing with the tailing spaces.
# Sometimes we have variable length cells, like userid:
# | username | userid | gender |
# |----------+--------+--------|
# | jonh     | 12     | male   |
# we need to match the 12 with a var $USERID which has been set by get_call().
# The output source will be something like:
# | username | userid | gender |
# |----------+--------+--------|
# | jonh     | userid1     | male   |
# to match it: match_sub userid1 $USERID
# but the problem here is the userid may change for different test executions. If we
# get a 3 digits userid like '123', the diff will fail since we have one more space than
# the actual sql output.
# To deal with it, use match_sub_tt userid $USERID
# And make the output source like:
# | username | userid | gender |
# |----------+--------+--------|
# | jonh     | userid1| male   |
# Notice here that there is no space following userid1 since we replace the whole userid with
# its tailing spaces with 'userid1'. Like '123   ' -> 'userid'.
match_sub_tt() {
    to_replace=""
    for var in "$@"
        do
        if [ -z "$to_replace" ]
        then
            to_replace=$var
        else
            # \b is trying to match the whole word to make it more stable.
            export MATCHSUBS="${MATCHSUBS}${NL}m/\\b${var}\\b/${NL}s/\\b${var} */${to_replace}/${NL}"
            to_replace=""
        fi
    done
    echo "${RAW_STR}"
}

# Substitute in the $RAW_STR and echo the result.
# Multi substitution pairs can be passed as arguments, like:
# sub "to_replace_1" "replacement_1" "to_replace_2" "replacement_2"
# This could be useful for both @in_sh and @out_sh. e.g.:
# @in_sh 'sub @TOKEN1 ${TOKEN1}': SELECT status FROM GP_ENDPOINTS_STATUS_INFO() WHERE token='@TOKEN1';
# Assume the $TOKEN has value '01234', The SQL will become:
# SELECT status FROM GP_ENDPOINTS_STATUS_INFO() WHERE token='01234';
sub() {
    to_replace=""
    for var in "$@"
        do
        if [ -z "$to_replace" ]
        then
            to_replace=$var
        else
            RAW_STR=$(echo "$RAW_STR" | sed -E "s/${to_replace}/${var}/g")
            to_replace=""
        fi
    done
    echo "${RAW_STR}"
}
