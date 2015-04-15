uwsgi --socket 127.0.0.1:8000 --wsgi-file service.py --callable app --enable-threads --buffer-size=32000 --check-static static
