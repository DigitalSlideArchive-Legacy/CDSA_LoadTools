### This will download an image set that is specified by a .JSON file
import girder_client
import os,sys
import json
from os.path import join as oj
import trainingSetHelpers as tsh

useGirderDefaultPaths = True
### SURPRISE ME!!!
## rootFolder = whatever else it defaults to...

imageDataDict = {}  ## This gets the data set definition and links to download the imageData
with open("TCGA.MultiClass.MacroImageSet.json","r") as fp:
    imageDataDict = json.load(fp)

### Set Default Parameters
thumbSize = 256
testSetName  = imageDataDict['meta']['testSetName']
startDir = os.path.expanduser('~/tcgaImageSet')  ### TO DO:  Make this a parmeter

## Connect to girder so I can actually download the images..
gc = girder_client.GirderClient(apiUrl=imageDataDict['serverAPIUrl'] )

###   Create root directory for this training set
downloadDir = os.path.join(startDir,testSetName,'macro',str(thumbSize))

imagesDownloaded = 0 ### Going to keep a counter for the downloaded images
for lbl in imageDataDict['meta']['cohortLabels']:
	print lbl

	## FUTURE VERSIONS... can specift thumbnail size and also macro vs tiles
	trainingOutputDir = os.path.join(downloadDir,"train",lbl)
	testingOutputDir = os.path.join(downloadDir,"test",lbl)
	validationOutputDir = os.path.join(downloadDir,"val",lbl)

	tsh.downloadImageSet( imageDataDict['trainingSet'][lbl], trainingOutputDir, gc)
	print "Training Set Downloaded %s" % lbl
	tsh.downloadImageSet( imageDataDict['testSet'][lbl], testingOutputDir,gc )
	print "Testing Set Downloaded for %s" % lbl
	tsh.downloadImageSet( imageDataDict['valSet'][lbl], validationOutputDir,gc )
	print "Downloaded validation set for %s" % lbl