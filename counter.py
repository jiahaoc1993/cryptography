import pyspark
from cryptography import decrypt_blob, encrypt_blob

sc = pyspark.SparkContext(appName="spark_redis_producer")

fd = open("private_key.pem", "rb")
private_key = fd.read()
fd.close()

fd = open("public_key.pem", "rb")
public_key = fd.read()
fd.close()

text_file = sc.textFile("./testfile_500_MB", minPartitions=10)
# counts = text_file.flatMap(lambda line: line.split(" ")) \
#             .map(lambda word: (word, 1)) \
#             .reduceByKey(lambda a, b: a + b)

bytes_text = text_file.mapPartitions(lambda x: ''.join(x))
encrypt_rdd = bytes_text.mapPartitions(lambda x: encrypt_blob(b''.join(x), publickey=public_key))
decrypt_rdd = encrypt_rdd.mapPartitions(lambda x: decrypt_blob(b''.join(x), private_key))
# encrypt_rdd = text_file.mapPartitions(lambda x: )
# print(text_file.sample(False, 0.1, 81).collect())
# flat_map = text_file.flatMap(lambda line: x.plit)
# print(b''.join(encrypt_rdd.collect()[:1000]))
# print(encrypt_rdd.getNumPartitions())

#print(''.join(text_file.collect()[:1000]))
#print(''.join(decrypt_rdd.collect()[:1000]))
#print(text_file.collect()[:100])
print(bytes_text.collect()[:10])
print(decrypt_rdd.collect()[:10])
# print(decrypt_rdd.getNumPartitions())
# print(text_file.getNumPartitions())
