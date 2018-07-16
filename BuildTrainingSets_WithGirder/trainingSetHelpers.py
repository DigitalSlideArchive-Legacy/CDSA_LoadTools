### Various helper functions we have generated to download/sort/process data from the 
### Girder Client for various TCGA Pathology training sets
import sys, os
from os.path import join as oj


thumbnailURL = "item/%s/tiles/thumbnail?width=%s" 

class LinePrinter():
   """
   Print things to stdout on one line dynamically
   """
   def __init__(self,data):
       sys.stdout.write("\r\x1b[K"+data.__str__())
       sys.stdout.flush()


def downloadImageSet( imageSet, downloadDir, girderClient, thumbSize=256 ):
    imagesDownloaded = 0
    gc=girderClient

    if not os.path.isdir(downloadDir):
        os.makedirs(downloadDir)

    for i in imageSet:
        ## I am using the tcga.barcode
        thumbName = i['tcga']['barcode']+".macro." + str(thumbSize)+".jpg"
        thumbWpath = oj(downloadDir,thumbName)
        imagesDownloaded +=1

        if not os.path.isfile(thumbWpath):
            curImage = gc.get( thumbnailURL % (i['_id'],thumbSize), jsonResp=False)
            with open(thumbWpath,"w") as fp:
                fp.write( curImage.content)
        LinePrinter("Have downloaded a total of %d images" % imagesDownloaded ) 
    print



