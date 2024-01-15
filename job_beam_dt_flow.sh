

EXPORT BUCKET=gs:<bucket-name>

gsutil mb -c STANDARD -l southamerica-east1 $BUCKET

python -m apache_beam.examples.wordcount --project $DEVSHELL_PROJECT_ID \
  --runner DataflowRunner \
  --staging_location $BUCKET/staging \
  --temp_location $BUCKET/temp \
  --template_location $BUCKET/templates/<name-for-your-template-file> \
  --output $BUCKET/results/output \
  --region <region>
