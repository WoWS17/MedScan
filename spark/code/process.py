from elasticsearch import Elasticsearch
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType
import openai
import os
import json
from elasticsearch import Elasticsearch

sc = SparkContext(appName="TAP-Project")
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

kafkaServer="kafkaServer:9092"
topic = "ner"

idx = 0
ELASTIC_INDEX = "ner_idx"
ELASTIC_HOST = "http://elasticsearch:9200"


SYSTEM_PROMPT = "\
                Sei un intelligente sistema che estrae delle caratteristiche da un testo.\
                Ti fornirò quali caratteristiche devi estrarre, il testo da cui estrarle e il formato di output.\
                "
USER_PROMPT_1 = "Hai chiaro il tuo ruolo?"
ASSISTANT_PROMPT_1 = "Certo, sono pronto ad aiutarti con il tuo compito. Ti prego di fornirmi le informazioni necessarie per iniziare."
GUIDELINES_PROMPT = (
"Formato Output:\n"
"{{\n"
    "\"Emiscroto destro\" : {{\n"
        "\"Testicolo\" : {{\n"
            "\"Sede anatomica\" : \"\",\n"
            "\"Volume\" : \"\",\n"
            "\"Dimensioni\" : \"\",\n"
            "\"Ecostruttura : \"\"\n"
        "}},\n"
        "\"Epididimo\" : {{\n"
            "\"Aspetto ecografico\" : \"\"\n"
        "}},\n"
        "\"Canale inguinale\" : \"\"\n"
    "}},\n"
    "\n"
    "\"Emiscroto sinistro\" : {{\n"
        "\"Testicolo\" : {{\n"
            "\"Sede anatomica\" : \"\",\n"
            "\"Volume\" : \"\",\n"
            "\"Dimensioni\" : \"\",\n"
            "\"Ecostruttura : \"\"\n"
        "}},\n"
        "\"Epididimo\" : {{\n"
            "\"Aspetto ecografico\" : \"\"\n"
        "}},\n"
        "\"Canale inguinale\" : \"\"\n"
    "}}\n"
"}}\n"
"Spiegazione caratteristiche :\n"
"\"Sede anatomica\" può assumere solo i valori \"Fisiologica\" o \"Non fisiologica\" o \"\"\n"
"\"Volume\" deve contenere il valore specificato nel referto. Non includere l'unità di misura e altri simboli come < o >. Se non è presente un valore univoco non inserire nulla.In italiano per indicare le cifre decimali usiamo la virgola; Sostituisci la virgola con il punto perchè devo lavorare con la notazione inglese.\n"
"\"Dimensioni\" può assumere solo i valori \"Normali per età\" o \"Non normali per età\" o \"\"\n"
"\"Ecostruttura\" può assumere solo i valori \"Omogenea\" o \"Disomogenea\" o \"\"\n"
"\"Aspetto ecografico\" può assumere solo i valori \"Normale\" o \"Anormale\" o \"\"\n"
"\"Canale inguinale\" può assumere solo i valori \"Normale\" o \"Anormale\" o \"\"\n"
"Nota che le stesse caratteristiche sono ripetute sia per l'emiscroto destro che per l'emiscroto sinistro, non fare confusione tra le due parti."
"Testo : {}\n"
"Output : "
)

'''ES_MAPPING = {
  "mappings": {
    "properties": {
        "Emiscroto destro": {
            "type": "nested",
            "properties": {
                "Testicolo": {
                    "type": "nested",
                    "properties": {
                        "Sede anatomica": {"type": "text"},
                        "Volume": {"type": "double"},
                        "Dimensioni": {"type": "text"},
                        "Ecostruttura": {"type": "text"}
                    }
                },
                "Epididimo": {
                    "type": "nested",
                    "properties": {
                        "Aspetto ecografico": {"type": "text"}
                    }
                },
                "Canale inguinale": {"type": "text"}
            }
        },
        "Emiscroto sinistro": {
            "type": "nested",
            "properties": {
                "Testicolo": {
                    "type": "nested",
                    "properties": {
                        "Sede anatomica": {"type": "text"},
                        "Volume": {"type": "double"},
                        "Dimensioni": {"type": "text"},
                        "Ecostruttura": {"type": "text"}
                    }
                },
                "Epididimo": {
                    "type": "nested",
                    "properties": {
                        "Aspetto ecografico": {"type": "text"}
                    }
                },
                "Canale inguinale": {"type": "text"}
            }
        }
    }
  }
}'''

def setOpenAIConf():
    openai.api_type = "azure"
    openai.api_base = os.getenv("AZURE_OPENAI_ENDPOINT")
    openai.api_version = "2023-03-15-preview"
    openai.api_key = os.getenv("AZURE_OPENAI_KEY")

def create_elastic_index():
    es = Elasticsearch(hosts = ELASTIC_HOST)
    response = es.indices.create(index=ELASTIC_INDEX, ignore=400)

    if 'acknowledged' in response:
        if response['acknowledged'] == True:
            print ("INDEX MAPPING SUCCESS FOR INDEX:", response['index'])
    return es
    
def requestToChatGPT(user_prompt):
   setOpenAIConf()
   return openai.ChatCompletion.create(
      engine="healthcare-features-extractor",
      messages = [
        {"role":"system","content":SYSTEM_PROMPT},
        {"role":"user","content":USER_PROMPT_1},
        {"role": "assistant", "content": ASSISTANT_PROMPT_1},
        {"role": "user", "content": user_prompt}
        ],
      temperature=0,
      max_tokens=800,
      top_p=0.95,
      frequency_penalty=0,
      presence_penalty=0,
      stop=None)

def sendToEs(batch_df: DataFrame, batch_id: int):
  for row in batch_df.rdd.collect():
    user_prompt = GUIDELINES_PROMPT.format(row['message'])
    print(row['message'])
    
    response = requestToChatGPT(user_prompt)
    response = response['choices'][0]['message']['content']
    print(response)
    if response[0] != '{':
        continue

    try:
        doc = json.loads(response.rsplit('}', 1)[0] + '}')
        doc["Emiscroto destro"]["Testicolo"]["Volume"] = float(doc["Emiscroto destro"]["Testicolo"]["Volume"])
        doc["Emiscroto sinistro"]["Testicolo"]["Volume"] = float(doc["Emiscroto sinistro"]["Testicolo"]["Volume"])
    except (json.JSONDecodeError, ValueError):
        continue
    
    doc["Timestamp"] = row["timestamp"]

    global idx
    idx = idx + 1
    resp = es.index(index=ELASTIC_INDEX, id=idx, document=doc)
    print(resp['result'])

es = create_elastic_index()

# Read from Kafka
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafkaServer) \
  .option("subscribe", topic) \
  .load()
  # DataFrameReader.load() returns a DataFrame

# Define the schema of the JSON data from Kafka
json_schema = StructType() \
    .add("tags", ArrayType(StringType())) \
    .add("message", StringType()) \
    .add("process", StructType()
         .add("exit_code", StringType())
         .add("command_line", StringType())) \
    .add("@version", StringType()) \
    .add("@timestamp", StringType()) \
    .add("host", StructType()
         .add("name", StringType())) \
    .add("event", StructType()
         .add("original", StringType()))

df2 = df.select(from_json(col("value").cast("string"), json_schema).alias("data"))
df3 = df2.select(col("data.@timestamp").alias("timestamp"), col("data.message").alias("message"))
# df3 ha 2 colonne: timestamp e message

# Write to Elastic Search
df3.writeStream \
    .foreachBatch(sendToEs) \
    .start() \
    .awaitTermination()


'''from elasticsearch import Elasticsearch
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType
import openai
import os
import json

sc = SparkContext(appName="TAP-Project")
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

idx = 0
es_index = "ner_idx"
es_address = "http://elasticsearch:9200"
es = Elasticsearch(hosts = es_address, verify_certs=False)

kafkaServer="kafkaServer:9092"
topic = "ner"

openai.api_type = "azure"
#openai.api_base = "https://ner-healthcare.openai.azure.com/"
openai.api_base = os.getenv("AZURE_OPENAI_ENDPOINT")
openai.api_version = "2023-03-15-preview"
openai.api_key = os.getenv("AZURE_OPENAI_KEY")
#openai.api_key = "5c7ea19777114ad2a834fe71c3d7d623"

SYSTEM_PROMPT = " \
                Sei un intelligente sistema che estrae delle caratteristiche da un testo.\
                Ti fornirò quali caratteristiche devi estrarre, il testo da cui estrarle e il formato di output.\
                "
USER_PROMPT_1 = "Hai chiaro il tuo ruolo?"

ASSISTANT_PROMPT_1 = "Certo, sono pronto ad aiutarti con il tuo compito. Ti prego di fornirmi le informazioni necessarie per iniziare."

GUIDELINES_PROMPT = (
"Formato Output:\n"
"{{\n"
    "\"Emiscroto destro\" : {{\n"
        "\"Testicolo\" : {{\n"
            "\"Sede anatomica\" : \"\",\n"
            "\"Volume\" : \"\",\n"
            "\"Dimensioni\" : \"\",\n"
            "\"Ecostruttura : \"\"\n"
        "}},\n"
        "\"Epididimo\" : {{\n"
            "\"Aspetto ecografico\" : \"\"\n"
        "}},\n"
        "\"Canale inguinale\" : \"\"\n"
    "}},\n"
    "\n"
    "\"Emiscroto sinistro\" : {{\n"
        "\"Testicolo\" : {{\n"
            "\"Sede anatomica\" : \"\",\n"
            "\"Volume\" : \"\",\n"
            "\"Dimensioni\" : \"\",\n"
            "\"Ecostruttura : \"\"\n"
        "}},\n"
        "\"Epididimo\" : {{\n"
            "\"Aspetto ecografico\" : \"\"\n"
        "}},\n"
        "\"Canale inguinale\" : \"\"\n"
    "}}\n"
"}}\n"
"Spiegazione caratteristiche :\n"
"\"Sede anatomica\" può assumere solo i valori \"Fisiologica\" o \"Non fisiologica\" o \"\"\n"
"\"Volume\" deve contenere il valore specificato nel referto. Non includere l'unità di misura. Se non è presente un valore inserisci -1.\n"
"\"Dimensioni\" può assumere solo i valori \"Normali per età\" o \"Non normali per età\" o \"\"\n"
"\"Ecostruttura\" può assumere solo i valori \"Omogenea\" o \"Disomogenea\" o \"\"\n"
"\"Aspetto ecografico\" può assumere solo i valori \"Normale\" o \"Anormale\" o \"\"\n"
"\"Canale inguinale\" può assumere solo i valori \"Normale\" o \"Anormale\" o \"\"\n"
"Nota che le stesse caratteristiche sono ripetute sia per l'emiscroto destro che per l'emiscroto sinistro, non fare confusione tra le due parti."
"Testo : {}\n"
"Output : "
)

def sendToEs(batch_df: DataFrame, batch_id: int):
  for row in batch_df.rdd.collect():
    user_prompt = GUIDELINES_PROMPT.format(row['message'])
    #print(row['message'])
    response = openai.ChatCompletion.create(
      engine="ner-distribution",
      messages = [
        {"role":"system","content":SYSTEM_PROMPT},
        {"role":"user","content":USER_PROMPT_1},
        {"role": "assistant", "content": ASSISTANT_PROMPT_1},
        {"role": "user", "content": user_prompt}
        ],
      temperature=0,
      max_tokens=800,
      top_p=0.95,
      frequency_penalty=0,
      presence_penalty=0,
      stop=None)
    print(response['choices'][0]['message']['content'])
    doc = json.loads(response['choices'][0]['message']['content'].rsplit('}', 1)[0] + '}')
    
    global idx
    idx = idx + 1
    resp = es.index(index=es_index, id=idx, document=doc)
    print(resp['result'])

# Read from Kafka
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafkaServer) \
  .option("subscribe", topic) \
  .load()
  # DataFrameReader.load() returns a DataFrame

# Define the schema of the JSON data from Kafka
json_schema = StructType() \
    .add("tags", ArrayType(StringType())) \
    .add("message", StringType()) \
    .add("process", StructType()
         .add("exit_code", StringType())
         .add("command_line", StringType())) \
    .add("@version", StringType()) \
    .add("@timestamp", StringType()) \
    .add("host", StructType()
         .add("name", StringType())) \
    .add("event", StructType()
         .add("original", StringType()))

df2 = df.select(from_json(col("value").cast("string"), json_schema).alias("data"))
df3 = df2.select(col("data.@timestamp").alias("timestamp"), col("data.message").alias("message"))

# df3 ha 2 colonne: timestamp e message

# Write to Elastic Search
df3.writeStream \
    .foreachBatch(sendToEs) \
    .start() \
    .awaitTermination()'''