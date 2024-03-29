import os

# this mess of path finding should always set the working directory to the parent (project) folder so
# all file paths work correctly (and dynamically!) regardless of file used
os.chdir(os.path.join(os.path.join(__file__, os.pardir), os.pardir))

ROOT_DIR = os.getcwd()

DATA_PATH = "Data/"

LIB_PATH = "lib/"

CHECKPOINT_PATH = DATA_PATH + "Checkpoints/"

CLERICAL_PATH = DATA_PATH + "Clerical files/"

OUTPUT_PATH = DATA_PATH + "Outputs/"

CENSUS_FILE_NAME = "Mock_Rwanda_Data_Census.csv"
PES_FILE_NAME = "Mock_Rwanda_Data_Pes.csv"
CENSUS_FILE_PATH = DATA_PATH + CENSUS_FILE_NAME
PES_FILE_PATH = DATA_PATH + PES_FILE_NAME
