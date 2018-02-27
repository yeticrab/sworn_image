# -*- coding: utf-8 -*-
"""
Created on Mon Feb 26 11:27:35 2018

Get image blobs, store in gc and add labels

@author: roger.gill
"""

import os, tempfile, shutil
import pandas as pd

import google_streetview.api
from google.cloud import vision
from google.cloud.vision import types
from google.cloud import storage

config = {'heading' : 0, 'pitch' : 10, 'size': '600x300'}

def street_view_image_analytics(postcode_profile, image_config):
    
    coords    = str(postcode_profile['latitude'][0]) + ',' + str(postcode_profile['longitude'][0])
    postcode  = postcode_profile['postcode'][0]
    local_dir = tempfile.tempdir + '\\' + postcode
    
    # Define parameters for street view api
    image_params = [{
           'size'      : image_config['size']
           ,'location' : coords
           ,'heading'  : image_config['heading']
           ,'pitch'    : image_config['pitch']
           ,'key'      : API_KEY
           }]
    
    # Create a results object
    results = google_streetview.api.results(image_params)
    results.download_links(local_dir)
    
    # Upload to storage
    storage_client  = storage.Client()
    bucket          = storage_client.bucket('sworn')
    
    for file in os.listdir(local_dir):
        source_filename = "{0}/{1}".format(local_dir, file)
        filename        = "sworn_image/{0}/{1}".format(postcode, file)
        if not bucket.get_blob(filename):
            blob            = bucket.blob(filename)
            blob.upload_from_filename(source_filename)
    
    jpg         = [f for f in os.listdir(local_dir) if f.endswith('.jpg')][0]
    s3_uri      = "gs://sworn/sworn_image/{0}/{1}".format(postcode, jpg)
    
    # Now analyse the image
    image_client           = vision.ImageAnnotatorClient()
    image                  = types.Image()
    image.source.image_uri = s3_uri
    response               = image_client.label_detection(image=image)
    
    image_labels = pd.DataFrame()
    for record in response.label_annotations:
        result = pd.DataFrame({
                'mid'          : record.mid
                ,'description' : record.description
                ,'score'       : record.score
                ,'topicality'  : record.topicality
                }, index = [0])
        
        image_labels = image_labels.append(result)
    
    image_labels     = image_labels.reset_index(drop=True)
    image_label_file = "{0}/{1}".format(local_dir, 'image_meta_data.csv')
    image_labels.to_csv(image_label_file, index = False)
    print(image_label_file)
    
    blob = bucket.blob("sworn_image/{0}/{1}".format(postcode, 'image_meta_data.csv'))
    blob.upload_from_filename(image_label_file, content_type = 'text/csv')
    shutil.rmtree(local_dir)
    
    return response




response = street_view_image_analytics(known_profiles.iloc[[0]], config)

'''

query = """
select
  *
from
`sandpit.known_profile_statistics`
"""
known_profiles = pd.read_gbq(query, 'datacrab-186315', dialect = 'standard')
known_profiles = known_profiles.dropna()