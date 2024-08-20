from fdfs_client.client import Fdfs_client

client = Fdfs_client('/home/zym/container/proj0_toolbox/djangotoolbox/utils/fastdfs/client.conf')
result = client.upload_by_filename('LICENSE')
print(result)
