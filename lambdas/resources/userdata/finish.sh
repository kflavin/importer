#################################################################################################################
# Finish script for userdata.  No shebang line is needed - this is merged with other scripts!
#
# total_time: calculates total run time, which is passed to the cleanup handler in the setup.
#################################################################################################################

end=$(date +%s)
export total_time=$((end-start))

# Terminate the instance.  Give extra time for logs to sync.
sleep 10