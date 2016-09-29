# s3
Amazon S3 backup plugin. Use this plugin to put/get files into amazon s3 storage

### API
* init(access_key, secret_key, region_name, host)
* download_file(bucket_name, key, filename)
* upload_file(bucket_name, key, filename)
* list(bucket: optional) - list bucket or service

### Usage
```lua
s3 = require('s3')

-- set credentals, region name and server
s3:init(
    'my_access_key', 
    'my_secret_key',
    'us-east-1',
    'hostname.com'
)

-- download file to current directory from s3 storage
if s3:download_file('files', 'file.txt', 'cloud_file.txt') then
    print('download complete')
end

-- upload backup to s3
if s3:upload_file('files', 'backup.txt', '/home/user/backup.txt') then
    print('upload complete')
end

print(require('yaml').encode(s3:list()))
---
- name: builds
  date: 2016-09-18T15:29:52Z
- name: debrepo
  date: 2016-09-18T15:18:30Z
- name: dist
  date: 2016-09-28T17:01:47Z
- name: play
  date: 2016-09-08T14:22:25Z
- name: test
  date: 2016-09-28T17:00:30Z
...

files = s3:list("test")
```

### Building and requirements
* libs3 fork (https://github.com/Sulverus/libs3), depends on libcurl and libxml2
