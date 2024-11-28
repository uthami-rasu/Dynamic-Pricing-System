from functools import wraps 
from datetime import datetime ,timedelta 
import traceback 
import json 
import logging
from watchtower import CloudWatchLogHandler

# Gloabl dictionary to store logger instances
GlobalLogger = {} 
def createOrGetLogger(logger_name, log_group="stream-processing", log_level=logging.DEBUG):
    """
    Creates or retrieves a custom logger with the specified name and configuration.

    This function ensures a centralized logging mechanism by storing and reusing loggers in the `GlobalLogger` dictionary.
    It is designed to integrate with cloud services like AWS CloudWatch (if additional handlers are added).

    Returns:
        logging.Logger: A logger instance configured with the specified parameters.
    """


    global GlobalLogger
    if not GlobalLogger.get(logger_name):
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)
        
        log_stream = f"{logger_name}/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        if not logger.hasHandlers():
            try:
                handler = CloudWatchLogHandler(log_group=log_group, stream_name=log_stream)
                handler.setFormatter(formatter)
                logger.addHandler(handler)
            except Exception as e:
                logger.error(f"Failed to create CloudWatch handler: {e}")
        GlobalLogger[logger_name] = logger
    
    return GlobalLogger.get(logger_name)



class Logger:
    """
    Logger class that includes a decorator to log details of function execution.

    This class provides a utility method `log` to wrap functions and log their entry, exit, 
    and execution details. It is useful for monitoring and debugging function behavior.
    """
    log_id = 1
    logs = {}
    log_structure = {
            "function_name": "",
            "status": "Success",
            "doc_string": "",
            "start_time": "",
            "end_time": "",
            "execution_time": "",
            "error": "Not occurred",
            "exception": "Not occurred",
        }
    @classmethod
    def log(cls,func):
        """
        Decorator function to log details of the wrapped function execution.
        """
        self =  cls 
        @wraps(func)  # Preserve the wrapped function's metadata
        def decorator(*args, **kwargs):

            start = datetime.now() + timedelta(hours=5, minutes=30)

            # Create a new log entry with a fresh copy of the log structure
            self.logs[f"log-id:{self.log_id:04d}"] = self.log_structure.copy()
            try:
                # Execute the wrapped function
                result = func(*args, **kwargs)
            except Exception as e:
                # Log exception details
                cache_exception = f"{traceback.format_exc()}"
                self.logs[f"log-id:{self.log_id:04d}"].update(
                    error=str(e), exception=cache_exception, status="Failed"
                )
                raise  # Re-raise the error for further handling
            finally:
                
                end = datetime.now() + timedelta(hours=5, minutes=30)
                # Calculate the execution time
                execution_time = end - start

                # Update the log entry with execution details
                self.logs[f"log-id:{self.log_id:04d}"].update(
                    function_name=func.__name__ or " ",
                    doc_string=json.dumps(func.__doc__, indent=4)
                    if func.__doc__
                    else "No Docstring Available",
                    start_time=start.strftime("%Y-%m-%d %H:%M:%S"),
                    end_time=end.strftime("%Y-%m-%d %H:%M:%S"),
                    execution_time=f"{execution_time.total_seconds()} seconds",
                )
                # Increment the unique log ID for the next log entry
                self.log_id += 1
            return result

        return decorator
    @classmethod
    def clear_logs(cls):
        cls.logs.clear() 


# @Logger.log
# def add(x):
#     print(x)


# @Logger.log 
# def sub(y):
#     print(y) 

# add(1)
# sub(2)

# print(Logger.logs)

