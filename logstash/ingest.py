import os
from glob import glob
from striprtf.striprtf import rtf_to_text
import time 

time.sleep(5) # Forse da aumentare per evitare che logstash mandi a kafka prima che spark abbia attivato lo streamReader
directory_path = "/eco-inguine"

# Get a list of all .rtf files in the directory
rtf_files = glob(os.path.join(directory_path, "*.rtf"))

i = 1
separator = "---"

for file_path in rtf_files:
    if i > 10:
        break
    with open(file_path, "r") as file:
        text = rtf_to_text(file.read())
        print(text)
        print(separator)
    i += 1
