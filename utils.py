import requests
import io
import gzip

def read_gzip_resource(url, url_params=None):
    web_response = requests.get(
        url, params=url_params, timeout=30, stream=True)
    csv_gz_file = web_response.content 
                                
    fileobj = io.BytesIO(csv_gz_file)
    with gzip.GzipFile(fileobj=fileobj) as file_lines:
        for line in file_lines:
            yield line