#### importer les modules
import os
import pprint as pp
### Définir les varibles d'environnoment

os.environ['envn']= 'TEST'
os.environ['header']= 'True'
os.environ['inferSchema']= 'True'

envn = os.environ['envn']
header= os.environ['header']
inferSchema=os.environ['inferSchema']

### Définir le reste des variables
appName="USA Prescriber research Report"
## Le chemin du projet
#print(os.getcwd())
current_path = os.getcwd()

csv_directory = "C:/Users/fabassi/PycharmProjects/pythonProject2/src/main/python/staging//fact"
parquet_directory  = "C:/Users/fabassi/PycharmProjects/pythonProject2/src/main/python/staging//dimension_city"
processed_files_directory = "C:/Users/fabassi/PycharmProjects/PysparkPorjectOOP/src//python"