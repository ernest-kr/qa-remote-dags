es_host = "http://test660:c1iasZ9utdoOTHjnB49rmU5qwxv4v4iz@astronomer-elasticsearch-nginx.astronomer:9200"
import re

url_components = re.split(":|//|@", es_host)
print(url_components)
url_components[4] = url_components[4].replace("-nginx", "")
print(url_components[4])
