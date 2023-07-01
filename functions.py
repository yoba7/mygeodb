import requests
import os
import zipfile
import logging
import sys
from datetime import date

def download(url: "url",destination: "path",if_exists='exit',proxies=None) -> bool:

    """
    
    Downloads a file from the internet.
    
    :param url: The file location on the internet
    :type  url: url (string)
	
    :param destination: Where to put the file
    :type  destination: path
	
    :param if_exists: What to do if the destination file already exists ? 
       If ``if_exists='exit'``, then the process is aborted before the download even starts.
       If ``if_exists='replace'``, then the destination file is replaced by the download.
    :type  if_exists: {'exit','replace'}, default='exit'
	
    :return: True if the operation succeeded; False otherwise.
    :rtype: bool
          
    """
    
    if os.path.exists(destination) and if_exists=='exit':
        logging.warning(f'Request aborted: {destination} already exists.')
        return False
    
    try:
        with open(destination, 'w') as f:
            pass # Check whether we can open the file with write access
    except IOError as e:
        logging.error(f'Impossible to create {destination}')
        return False
    
    logging.info(f'Download of {url} starts')
    
    if proxies:
        r = requests.get(url, allow_redirects=True,proxies=proxies)
    else:
        r = requests.get(url, allow_redirects=True)
    
    if r.status_code!=200:
        logging.error(f'Request failed with status code {r.status_code}.')
        return False
    else:
        with open(destination, 'wb') as f:
            f.write(r.content)
            logging.info(f'Download of {url}->{destination} completed successfully.')
        return True


def unzip(zipped: 'path',unzipped: 'path') -> None:

    """
    
    Unzips an archive to a directory.
    
    :param zipped: Path to the zipped archive
    :type  zipped: path
        
    :param unzipped: Directory to unzip the file    
    :type  unzipped: path

    :return: None

    """
    with zipfile.ZipFile(zipped, 'r') as myzip:
        myzip.extractall(unzipped)
        
    return None
    
    
def getLogger(logFile):

    """
      
    Get a logger. Log messages are sent both to the standard output and to a logFile.
    
    :param logFile: The path to the log file. Eg: ./log/myProgram. 
        The iso date + .log extension will be added to the path. 
    :type  logFile: path    
        
        
    :return: The logger
    :rtype: Logger
    
    
    :example:
    
       >>> from statbel.functions import getLogger
       >>> logging=getLogger('./log/myProgram') 
       >>> logging.info('This is an info line') 
    
    """
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s|%(levelname)s|%(message)s')

    standardOutputHandler = logging.StreamHandler(sys.stdout)
    standardOutputHandler.setLevel(logging.INFO)
    standardOutputHandler.setFormatter(formatter)

    fileHandler=logging.FileHandler(f'{logFile}-{date.today().isoformat()}.log')
    fileHandler.setLevel(logging.INFO)
    fileHandler.setFormatter(formatter)

    root.addHandler(standardOutputHandler)
    root.addHandler(fileHandler)
    return(logging)
