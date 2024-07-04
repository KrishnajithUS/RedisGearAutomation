import time

# Function to create index if it doesn't exist
def create_index():
    try:
        execute('FT.CREATE', 'transaction_idx',
            'ON', 'HASH',
            'PREFIX', '1', 'transaction:',
            'SCHEMA', 'audioPlayed', 'NUMERIC',
                     'tMsgRecvByServer', 'NUMERIC')
        log("Index created")
    except Exception as e:
        if "Index already exists" in str(e):
            log("Index already exists")
        else:
            log("Error creating index: " + str(e))

# Function to run the search and apply logic
def process_transactions(x):
    log("Inside transaction")
    create_index()

    # Current time and expiry time
    current_time = int(time.time())
    expiry_time = 300  # Example expiry time, you can adjust this
    curr_t = current_time - expiry_time

    # Minimum value for audioPlayed
    min_audioPlayed = 0

    # Define the total number of entries to process
    log("count",execute('FT.SEARCH', 'transaction_idx', f"@audioPlayed:[{min_audioPlayed} +inf] | @tMsgRecvByServer:[{curr_t} +inf]", 'LIMIT', str(0), str(0))[0])
    total_entries = int(execute('FT.SEARCH', 'transaction_idx', f"@audioPlayed:[{min_audioPlayed} +inf] | @tMsgRecvByServer:[{curr_t} +inf]", 'LIMIT', str(0), str(0))[0])

    # Define batch size for search results
    batch_size = 1000  # You can adjust this value as needed

    # Initialize offset
    offset = 0

    # Iterate through the results in batches
    start_time = time.time()
    while offset < total_entries:
        # Search query for transaction_idx index with pagination
        log("---------------------------------------start search--------------------------------------------------")

        search_result = execute('FT.SEARCH', 'transaction_idx', f"@audioPlayed:[{min_audioPlayed} +inf] | @tMsgRecvByServer:[{curr_t} +inf]", 'LIMIT', str(offset), str(batch_size))
        
        log("---------------------------------------end search--------------------------------------------------")
        # Log the time taken for the search
        log(f"Search results: {search_result}")

        # Update offset for the next batch
        offset += batch_size

        # If no more results, break the loop
        if len(search_result) == 1:  # Only the number of results returned
            break

    end_time = time.time()
    log(f"Search completed in {end_time - start_time} seconds")
    log("Pagination completed")

    return 

# Register the function as a Gears function to be triggered by a cron job or some other mechanism
GearsBuilder().map(lambda x: process_transactions(x)).run("person:*")
