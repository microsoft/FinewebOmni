# Copyright (c) Microsoft Corporation.
from loguru import logger

# Define a filter that excludes WARNING messages
def exclude_warnings(record):
    return record["level"].name != "WARNING"

# Add a sink (e.g., stdout) with the filter applied
logger.add(lambda m: m, filter=exclude_warnings)

# Test logging at different levels
logger.debug("This is a debug message.")    # Will be shown
logger.info("This is an info message.")     # Will be shown
logger.warning("This is a warning message.")# Will NOT be shown
logger.error("This is an error message.")   # Will be shown