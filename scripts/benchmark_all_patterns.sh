#!/bin/bash

trap "echo exited!; exit;" SIGINT SIGTERM

export LC_ALL=en_US.utf-8;
export LANG=en_US.utf-8;

BASEDIR=/sharedstorage/ykerdcha/code/query_benchmark/script/;
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(realpath $BASEDIR/../deps/GraphBLAS/build/);

EXCEPTION="(^.*orkut_adj.*$)"
CONTAIN="(^.*(ca-GrQc)_adj_IA.*$)"
# CONTAIN="(^.*(ca-GrQc|facebook|cit-Patents)_adj_IA.*$)"
# CONTAIN="(^.*(flickrEdges|roadNet-PA|cit-Patents|ca-GrQc|facebook|oregon1_010526|oregon2_010526|p2p-Gnutella30|email-Enron|as20000102|as-caida20071105|cit-HepTh|email-EuAll|amazon0302|soc-Epinions1|loc-brightkite_edges)_adj_IA.*$)"
DATE=$(date -d "today" +"%Y%m%d%H%M")
SERVER_LOG="/sharedstorage/ykerdcha/data/bfs-se-la/e2e/rg_server_$DATE.log"
CLIENT_LOG="/sharedstorage/ykerdcha/data/bfs-se-la/e2e/rg_client_$DATE.log"

# Mode 1: ORIGINAL
# Mode 2: FUSED_FILTER_AND_TRAVERSE
# Mode 3: CN_ACCUMULATE_SELECT
# Mode 4: CN_MXM_LIKE

unset TMUX;
rm *.rdb

for query in 0 1 2 3 4 5
do
    for mode in 0 1 2 3 4 5 6 7 8 9
    do
        # Make mode
        python3 scripts/pattern_change.py "$mode" "$query";
        make;
        for num_threads in 24;
        # for num_threads in 1 24;
        do
            for file in /sharedstorage/markb1/ktruss_data/unsorted_bin/*;
            do
                if [[ "$file" =~ $CONTAIN ]] && [[ ! "$file" =~ $EXCEPTION ]]
                then
                    graph_name=$(echo $(basename $file) | sed 's/_adj_IA.txt.bin//g');

                    # run redis-server
                    echo "🔄 Starting redis-server...";
                    tmux new-session -d -s "redisgraph";
                    tmux send-keys -t "redisgraph" "sh ~/run-redisgraph $num_threads >> $SERVER_LOG" C-m;
                    echo "✅ Done!";
                    
                    # sleep to make sure that redis-server is started nicely
                    echo "🔄 Sleeping for 15 seconds...";
                    sleep 15;
                    echo "✅ Done!";

                    # bulkload
                    echo "🔄 (Potentially Deleting and) Bulkloading ${graph_name}...";
                    sh "$BASEDIR/bulkload_redisgraph.sh" "$graph_name";
                    echo "✅ Done!";

                    # sleep to make sure that redis-server is started nicely
                    echo "🔄 Sleeping for 15 seconds...";
                    sleep 15;
                    echo "✅ Done!";

                    echo "🔄 Running the benchmark for ${graph_name}...";
                    echo "<entry>" >> ${CLIENT_LOG};
                    echo "$graph_name | $query | $num_threads | $mode" >> ${CLIENT_LOG};
                    if [ "$mode" = "0" ] || [ "$mode" = "1" ]; then
                        if [ "$query" = "0" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c) WHERE a <> b AND a <> c AND b <> c RETURN [a,b,c]" \
                                >> ${CLIENT_LOG};
                        elif [ "$query" = "1" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (a)-->(c), (b)-->(c) WHERE a <> b AND a <> c AND b <> c RETURN [a,b,c]" \
                                >> ${CLIENT_LOG};
                        elif [ "$query" = "2" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d) WHERE a <> b AND a <> c AND a <> d AND b <> c AND b <> d AND c <> d RETURN [a,b,c,d]" \
                                >> ${CLIENT_LOG};
                        elif [ "$query" = "3" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d), (a)-->(d) WHERE a <> b AND a <> c AND a <> d AND b <> c AND b <> d AND c <> d RETURN [a,b,c,d]" \
                                >> ${CLIENT_LOG};
                        elif [ "$query" = "4" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d), (a)-->(d), (b)-->(d) WHERE a <> b AND a <> c AND a <> d AND b <> c AND b <> d AND c <> d RETURN [a,b,c,d]" \
                                >> ${CLIENT_LOG};
                        else
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (a)-->(c), (a)-->(d), (b)-->(c), (b)-->(d), (c)-->(d) WHERE a <> b AND a <> c AND a <> d AND b <> c AND b <> d AND c <> d RETURN [a,b,c,d]" \
                                >> ${CLIENT_LOG};
                        fi
                    elif [ "$mode" = "2" ]; then
                        if [ "$query" = "0" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c) RETURN [a,b,c]" \
                                >> ${CLIENT_LOG};
                        elif [ "$query" = "1" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (a)-->(c), (b)-->(c) RETURN [a,b,c]" \
                                >> ${CLIENT_LOG};
                        elif [ "$query" = "2" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d) RETURN [a,b,c,d]" \
                                >> ${CLIENT_LOG};
                        elif [ "$query" = "3" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d), (a)-->(d) RETURN [a,b,c,d]" \
                                >> ${CLIENT_LOG};
                        elif [ "$query" = "4" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d), (a)-->(d), (b)-->(d) RETURN [a,b,c,d]" \
                                >> ${CLIENT_LOG};
                        else
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (a)-->(c), (a)-->(d), (b)-->(c), (b)-->(d), (c)-->(d) RETURN [a,b,c,d]" \
                                >> ${CLIENT_LOG};
                        fi
                    else
                        if [ "$query" = "0" ] || [ "$query" = "1" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c) RETURN [a,b,c]" \
                                >> ${CLIENT_LOG};
                        else
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.profile "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d) RETURN [a,b,c,d]" \
                                >> ${CLIENT_LOG};
                        fi
                    fi
                    echo "</entry>" >> ${CLIENT_LOG};
                    echo "✅ Done!";
                    
                    # remove graph
                    echo "🔄 Removing ${graph_name}...";
                    ~/dbms/redis/src/redis-cli --raw graph.delete ${graph_name};
                    echo "✅ Done!";

                    # shutdown
                    echo "🔄 Shutting down redis-server...";
                    ~/dbms/redis/src/redis-cli shutdown;
                    echo "✅ Done!";

                    # sleep to make sure that redis-server is ended nicely
                    echo "🔄 Sleeping for 15 seconds...";
                    sleep 15;
                    tmux kill-session -t "redisgraph";
                    echo "✅ Done!";
                fi
            done
        done
    done;
done