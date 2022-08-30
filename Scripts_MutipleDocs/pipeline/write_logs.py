import logging
import configparser
import os


read_config = configparser.ConfigParser()
read_config.read('../../Config/central_config.ini')
print('../../Config/central_config.ini')

databank_location = read_config.get("databank", "databank_location")

 #= 'E:/MLOPSDataBank'
def writetolog(msg):
    print(databank_location)
    logging.basicConfig(filename=databank_location+'/Logs/Log.txt', filemode='a+', format='%(asctime)s --%(message)s')
    #print('Script Run Successfully')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.info(msg)

