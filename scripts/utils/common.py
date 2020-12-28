# from dateutil.relativedelta import relativedelta

# Method to get Time Difference
# def time_difference(t_a, t_b):
#     t_diff = relativedelta(t_b, t_a)  # later/end time comes first!
#     return '{h}h {m}m {s}s'.format(h=t_diff.hours, m=t_diff.minutes, s=t_diff.seconds)

# Method to raise exception
def throw_error(error):
    raise Exception(error)