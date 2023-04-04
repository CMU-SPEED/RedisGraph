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
DIFF_LOG="rg_mine_diff_$DATE.log"

# Mode 1: ORIGINAL
# Mode 2: FUSED_FILTER_AND_TRAVERSE
# Mode 3: CN_ACCUMULATE_SELECT
# Mode 4: CN_MXM_LIKE

unset TMUX;
rm *.rdb

for num_threads in 24;
# for num_threads in 1 24;
do
    for file in /sharedstorage/markb1/ktruss_data/unsorted_bin/*;
    do
        if [[ "$file" =~ $CONTAIN ]] && [[ ! "$file" =~ $EXCEPTION ]]
        then
            graph_name=$(echo $(basename $file) | sed 's/_adj_IA.txt.bin//g');
            for query in 4
            # for query in 0 1 2 3 4 5
            do
                # for mode in 0 1
                for mode in 0 1 2 3 4 5 6 7 8 9
                do
                    # Make mode
                    python3 scripts/pattern_change.py "$mode" "$query";
                    make;

                    # run redis-server
                    echo "🔄 Starting redis-server...";
                    tmux new-session -d -s "redisgraph";
                    tmux send-keys -t "redisgraph" "sh ~/run-redisgraph $num_threads" C-m;
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
                    if [ "$mode" = "0" ]; then
                        if [ "$query" = "0" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c) WHERE a <> b AND a <> c AND b <> c RETURN [a,b,c]" \
                                > "original.tmp";
                        elif [ "$query" = "1" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (a)-->(c), (b)-->(c) WHERE a <> b AND a <> c AND b <> c RETURN [a,b,c]" \
                                > "original.tmp";
                        elif [ "$query" = "2" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d) WHERE a <> b AND a <> c AND a <> d AND b <> c AND b <> d AND c <> d RETURN [a,b,c,d]" \
                                > "original.tmp";
                        elif [ "$query" = "3" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d), (a)-->(d) WHERE a <> b AND a <> c AND a <> d AND b <> c AND b <> d AND c <> d RETURN [a,b,c,d]" \
                                > "original.tmp";
                        elif [ "$query" = "4" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d), (a)-->(d), (b)-->(d) WHERE a <> b AND a <> c AND a <> d AND b <> c AND b <> d AND c <> d RETURN [a,b,c,d]" \
                                > "original.tmp";
                        else
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (a)-->(c), (a)-->(d), (b)-->(c), (b)-->(d), (c)-->(d) WHERE a <> b AND a <> c AND a <> d AND b <> c AND b <> d AND c <> d RETURN [a,b,c,d]" \
                                > "original.tmp";
                        fi
                    elif [ "$mode" = "1" ]; then
                        if [ "$query" = "0" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c) WHERE a <> b AND a <> c AND b <> c RETURN [a,b,c]" \
                                > "modified.tmp";
                        elif [ "$query" = "1" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (a)-->(c), (b)-->(c) WHERE a <> b AND a <> c AND b <> c RETURN [a,b,c]" \
                                > "modified.tmp";
                        elif [ "$query" = "2" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d) WHERE a <> b AND a <> c AND a <> d AND b <> c AND b <> d AND c <> d RETURN [a,b,c,d]" \
                                > "modified.tmp";
                        elif [ "$query" = "3" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d), (a)-->(d) WHERE a <> b AND a <> c AND a <> d AND b <> c AND b <> d AND c <> d RETURN [a,b,c,d]" \
                                > "modified.tmp";
                        elif [ "$query" = "4" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d), (a)-->(d), (b)-->(d) WHERE a <> b AND a <> c AND a <> d AND b <> c AND b <> d AND c <> d RETURN [a,b,c,d]" \
                                > "modified.tmp";
                        else
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (a)-->(c), (a)-->(d), (b)-->(c), (b)-->(d), (c)-->(d) WHERE a <> b AND a <> c AND a <> d AND b <> c AND b <> d AND c <> d RETURN [a,b,c,d]" \
                                > "modified.tmp";
                        fi
                    elif [ "$mode" = "2" ]; then
                        if [ "$query" = "0" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c) RETURN [a,b,c]" \
                                > "modified.tmp";
                        elif [ "$query" = "1" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (a)-->(c), (b)-->(c) RETURN [a,b,c]" \
                                > "modified.tmp";
                        elif [ "$query" = "2" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d) RETURN [a,b,c,d]" \
                                > "modified.tmp";
                        elif [ "$query" = "3" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d), (a)-->(d) RETURN [a,b,c,d]" \
                                > "modified.tmp";
                        elif [ "$query" = "4" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d), (a)-->(d), (b)-->(d) RETURN [a,b,c,d]" \
                                > "modified.tmp";
                        else
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (a)-->(c), (a)-->(d), (b)-->(c), (b)-->(d), (c)-->(d) RETURN [a,b,c,d]" \
                                > "modified.tmp";
                        fi
                    else
                        if [ "$query" = "0" ] || [ "$query" = "1" ]; then
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c) RETURN [a,b,c]" \
                                > "modified.tmp";
                        else
                            ~/dbms/redis/src/redis-cli \
                                --raw graph.query "$graph_name" \
                                "MATCH (a)-->(b), (b)-->(c), (c)-->(d) RETURN [a,b,c,d]" \
                                > "modified.tmp";
                        fi
                    fi
                    echo "✅ Done!";

                    # compare
                    if [ "$mode" = "0" ]; then
                        sort -o "original.tmp" "original.tmp";
                    else
                        sort -o "modified.tmp" "modified.tmp";
                        echo "${graph}/${query}/${mode}" >> "$DIFF_LOG";
                        diff "original.tmp" "modified.tmp" >> "$DIFF_LOG";
                    fi
                    
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
                done;
            done;
        fi;
    done;
    rm "original.tmp";
    rm "modified.tmp";
done;



