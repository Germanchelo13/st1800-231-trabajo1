create external schema myspectrum_schema
from data catalog
database 'datos_atmosfericos'
iam_role 'arn:aws:iam::409340898205:role/LabRole'
create external database if not exists;