from connection import currtime

currtime = currtime()

def sperror(currerror, currid):
    strerror = """UPDATE analysis SET analysis_status = 'error',
                    error = '{current_error}',
                    modified = '{current_time}' 
                    WHERE id = {current_id}""".format(current_error=currerror, current_time=currtime, current_id=currid)

    return strerror