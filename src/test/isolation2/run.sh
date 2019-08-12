#!/bin/sh

gpconfig -c log_min_messages -v DEBUG4

ps -ef | grep post > pid_list.txt

pid=$(cat pid_list.txt | grep "postgres -D /Users/mchen/repo/gpdb/gpAux/gpdemo/datadirs/dbfast1" | cut -d' ' -f4)
tmux new -d -s "gdb_$pid" gdb -ex c -p $pid
pid=$(cat pid_list.txt | grep "postgres -D /Users/mchen/repo/gpdb/gpAux/gpdemo/datadirs/dbfast2" | cut -d' ' -f4)
tmux new -d -s "gdb_$pid" gdb -ex c -p $pid
pid=$(cat pid_list.txt | grep "postgres -D /Users/mchen/repo/gpdb/gpAux/gpdemo/datadirs/dbfast3" | cut -d' ' -f4)
tmux new -d -s "gdb_$pid" gdb -ex c -p $pid

./pg_isolation2_regress  --init-file=../../../src/test/regress/init_file --init-file=./init_file_parallel_cursor --psqldir='/Users/mchen/gp/bin' --inputdir=. --dbname=isolation2parallelcursor --schedule=./parallel_cursor_schedule --host=127.0.0.1
