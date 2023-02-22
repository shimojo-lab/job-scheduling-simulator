# convert second to hour:minute:second string
def formatted_time(time):
    hour = time // 3600
    minute = (time % 3600) // 60
    second = time % 60
    return f"{hour:02}:{minute:02}:{second:02}"
