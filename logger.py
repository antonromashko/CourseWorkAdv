def get_logger(name, file, log_lvl, app, encoding='utf8'):
    import sys
    import logging

    my_log = logging.getLogger(name)
    my_log.setLevel(log_lvl.upper())
    formatter = logging.Formatter(app.config['CM_LOGGING_FORMAT'])

    fh = logging.FileHandler(file, encoding=encoding)
    fh.setLevel(log_lvl.upper())

    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(log_lvl.upper())

    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    my_log.addHandler(fh)
    my_log.addHandler(ch)

    return my_log
