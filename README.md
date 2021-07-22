# bqdataflow

#find your env location - 

./gsutil ls -Lb gs://gcbucket-wordcount

#Command for runner

python localpipeline.py --input gs://gcbucket-wordcount/input.txt --runner=DataflowRunner --project=mydataflowtest --job_name=wordcount --temp_location=gs://gcbucket-wordcount/temp --region=us-west1

#Create service key for auth -

https://cloud.google.com/docs/authentication/production#auth-cloud-implicit-python
export GOOGLE_APPLICATION_CREDENTIALS="/Users/apple/Desktop/bqproject/mydataflowtest-xxcad.json"
