#!/bin/bash

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

# Generate $MATCHSUBS and echo the $RAWSTR based on the given original string and replacement pairs.
# Arg 1n (input): The original string to be replaced.
# Arg 2n (input): The replacement string.
# Example:
# 1: @out_sh 'match_sub $USER_NAME user1, $USER_ID, id1': SELECT id,name,status FROM users;
# here we assume $USER_NAME and $USER_ID has been set to 'Jonh' and '1' already. Then the above
# statement will generate $MATCHSUBS section:
# m/\bJonh\b/
# s/\bJonh\b/user1/
#
# madf
\b1\b/
# s/\b1\b/id1/
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
