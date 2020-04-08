# fill the form and copy this file name is 'config.py'
DATABASES = {
    'mysql': {
        'NAME': '',
        'USER': '',
        'PASSWORD': '',
        'HOST': '',
        'PORT': 3306
    }
}

FILE_DIR = {
    'checkin': {
        'json_file_dir': "/path/file/checkin.json",
        'csv_file_dir': "/path/file/checkin.csv",
        'ouput_file_dir': "/path/file/checkin.output"
    },
    'review': {
        'json_file_dir': "/path/file/review.json",
        'csv_file_dir': "/path/file/review.csv",
        'ouput_file_dir': "/path/file/review.output"
    },
    'business': {
        'json_file_dir': "/path/file/business.json",
        'csv_file_dir': "/path/file/business.csv",
        'ouput_file_dir': "/path/file/business.output"
    }
}