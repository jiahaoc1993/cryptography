## Usage
1. create 500MB file with
```
dd if=/dev/zero of=testfile bs=1024 count=502400
```

2. run encrypt.py

3. run decrypt.py

4. verify decrpyted file with
```
xdd -p -l100 testfile
```
