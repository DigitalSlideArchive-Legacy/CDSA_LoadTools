""" This contains various helper functions for the Digital Slide Archive"""
import re, csv, os, sys
#from PIL import Image
#import openslide
import hashlib
from functools import partial
#from openslide.lowlevel import OpenSlideError


def md5sum(filename):
    with open(filename, mode='rb') as f:
        d = hashlib.md5()
        for buf in iter(partial(f.read, 128), b''):
            d.update(buf)
    return d.hexdigest()

def clean_openslide_keys ( properties_dict ):
    """Openslide returns dictionaries that have . in the keys which mongo does not like, I need to change this to _"""
    cleaned_dict = {}
    for k,v in properties_dict.iteritems():
        new_key = k.replace('.','_')
        cleaned_dict[new_key] = v
    return cleaned_dict

def OpenslideGetImageMetadata(full_file_path):
    """This will use the openslide bindings to get the width, height and filesize for an image or return an Error otherwise"""
    width=height=filesize=orig_resolution=slide_title=md5 = None
    ## I am going to make the crazy assumption that if the file ends with .SVS it's an SVS
    ## and if it ends with .NDPI it's an.. NDPI
    ## This matters bcecause of the way certain image properties are mapped
    try:
        im = openslide.open_slide(full_file_path)
        (width, height) = im.dimensions
        base_file_name = os.path.basename(full_file_path)
        filesize = os.path.getsize(full_file_path)
        
        #print base_file_name,filesize,im
        
        if  base_file_name.endswith('svs') :
            try:
                orig_resolution = im.properties['aperio.AppMag']
            except:
                orig_resolution = 'UnkSVSReadError'
        elif base_file_name.endswith('ndpi'):
            try:
                orig_resolution = im.properties['openslide.objective-power']
            except:
                orig_resolution = 'UnkNDPIReadError'
            ###WIP:  This is very likely not true in all cases-- just happens to be true @ Emory
        else:
            """NEED TO ADD CODE TO OPEN OTHER FILE TYPES?? LIKE A TIFF.. NOT SURE WHAT HAPENS"""
            print "Can't open",base_file_name
            sys.exit()
        
        sldScan_properties = im.properties
        sldMetaData = { 'width': width, 'height': height, 'origResolution': orig_resolution,
                      'scanProperties': sldScan_properties}
        return(True,sldMetaData)
       
    except OpenSlideError, e:
        #print "Openslide returned an error",full_file_path
        #print >>sys.stderr, "Verify failed with:", repr(e.args)
        #print "Openslide returned an error",full_file_path
        #eclean = clean_openslide_keys(e), 'ErrorCode': eclean}
        return(False,{'FileWErrors': full_file_path, 'ErrorType': 'OpenSlideError'})
    except StandardError, e:
        #file name likely not valid
        #print >>sys.stderr, "Verify failed with:", repr(e.args)
        #print "Openslide returned an error om the StandardError block",full_file_path
#        eclean = clean_openslide_keys(e)
#                     , 'ErrorCode': eclean}
        return(False,{'FileWErrors': full_file_path, 'ErrorType': 'StandardError'})
    except:
        return(False,{'FileWErrors': full_file_path,'ErrorType': 'UnknownError'})
    
class LinePrinter():
    """
    Print things to stdout on one line dynamically
    """
    def __init__(self,data):
        sys.stdout.write("\r\x1b[K"+data.__str__())
        sys.stdout.flush()

        
        
WSI_Ext = ['.svs', '.ndpi']  ### These are extensions I should index

def get_filepaths(directory):
    """
    This function will generate the file names in a directory 
    tree by walking the tree either top-down or bottom-up. For each 
    directory in the tree rooted at directory top (including top itself), 
    it yields a 3-tuple (dirpath, dirnames, filenames).
    """
    file_paths = []  # List which will store all of the full filepaths.

    # Walk the tree.
    for root, directories, files in os.walk(directory):
        for filename in files:
            # Join the two strings in order to form the full filepath.
            ###WSI_root
            curFileExt = os.path.splitext(filename)[1]
            if curFileExt in WSI_Ext:
                filepath = os.path.join(root, filename)
                file_paths.append(filepath)  # Add it to the list.
    return file_paths  # Self-explanatory.
# Run the above function and store its results in a variable. 
