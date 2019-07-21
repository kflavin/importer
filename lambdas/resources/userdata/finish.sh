end=$(date +%s)
export total_time=$((end-start))

# Terminate the instance.  Give extra time for logs to sync.
sleep 10