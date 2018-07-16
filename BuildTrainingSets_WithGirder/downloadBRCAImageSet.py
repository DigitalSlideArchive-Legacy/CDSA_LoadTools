### This will download an image set that is specified by a .JSON file
import girder_client
import os,sys
import json
from os.path import join as oj

class LinePrinter():
   """
   Print things to stdout on one line dynamically
   """
   def __init__(self,data):
       sys.stdout.write("\r\x1b[K"+data.__str__())
       sys.stdout.flush()

imageDataDict = {}
with open("TCGA.BRCA.ImageSet.json","r") as fp:
    imageDataDict = json.load(fp)

thumbSize = 256
testSetName  = imageDataDict['meta']['testSetName']
print(imageDataDict.keys())
startDir = os.path.expanduser('~/tcgaImageSet')
label = 'brca'

## Will cycle through the cohortLabels

gc = girder_client.GirderClient(apiUrl=imageDataDict['serverAPIUrl'] )
downloadDir = os.path.join(startDir,testSetName,'macro',label,str(thumbSize))

print(downloadDir)

thumbnailURL = "item/%s/tiles/thumbnail?width=%s" 

## FUTURE VERSIONS... can specift thumbnail size and also macro vs tiles

trainingOutputDir = os.path.join(downloadDir,"train")
testingOutputDir = os.path.join(downloadDir,"test")
validationOutputDir = os.path.join(downloadDir,"val")
imagesDownloaded = 0 ### Going to keep a counter for the downloaded images


def downloadImageSet( imageSet, downloadDir ):
    imagesDownloaded = 0
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


downloadImageSet( imageDataDict['trainingSet'], trainingOutputDir )
print("Training Set Downloaded")
downloadImageSet( imageDataDict['testSet'], testingOutputDir )
print("Testing Set Downloaded")
downloadImageSet( imageDataDict['valSet'], validationOutputDir )
print("Downloaded validation set")

