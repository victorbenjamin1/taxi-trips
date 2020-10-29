import os

if not os.path.exists('files/datasets'):
    os.makedirs('files/datasets')

if not os.path.exists('files/results'):
    os.makedirs('files/results')

os.system('aws s3 sync s3://ravya-datasprints/trips/ files/datasets/')
os.system('aws s3 sync s3://ravya-datasprints/vendor-lookup/ files/datasets/')
os.system('aws s3 sync s3://ravya-datasprints/payment-lookup/ files/datasets/')

