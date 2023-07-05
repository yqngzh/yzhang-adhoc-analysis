import time
import pickle
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

data_path = "gs://training-dev-search-data-jtzn/user/aclapp/clip_embeddings/exploration/CLIP_05-21_05-29_full-listing/joint/out-*"

file = open("./data/sample_data.pkl", 'rb')
data_dic = pickle.load(file)
file.close()

all_listings = set()
for v in data_dic.values():
    all_listings = all_listings.union(set(v["purchased_listings"]))

def main():
    now = int(time.time())
    pipeline_options = PipelineOptions(
        save_main_session=True,
        pipeline_type_check=True,
        job_name=f"read_clip_embeddings-{now}",
    )
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | beam.io.ReadFromText(data_path)
            | beam.Filter(lambda x: x["listing_id"] in all_listings)
            | beam.io.WriteToText()
        )

__