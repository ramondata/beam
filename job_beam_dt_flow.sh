

EXPORT BUCKET=gs:<bucket-name>

gsutil mb -c STANDARD -l southamerica-east1 $BUCKET


##caso tenha subnet default:
python -m apache_beam.examples.wordcount --project $DEVSHELL_PROJECT_ID \
  --runner DataflowRunner \
  --staging_location $BUCKET/staging \
  --temp_location $BUCKET/temp \
  --template_location $BUCKET/templates/<name-for-your-template-file> \
  --output $BUCKET/results/output \
  --region <region>

##caso não tenha uma vpn default, necessita informar a url da subnet:
python -m from_bq_to_bucket \ 
  --subnet  https://www.googleapis.com/compute/v1/projects/cerc2-gestop-stg/regions/southamerica-east1/subnetworks/subnet-gestops-stg-02
