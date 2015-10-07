""" This contains various helper functions for the Digital Slide Archive"""
import re, csv, os, sys, optparse
import collections
from PIL import Image
import openslide
from openslide.lowlevel import OpenSlideError
import hashlib
import subprocess
import shutil,glob
import random
from functools import partial

def md5sum(filename):
    with open(filename, mode='rb') as f:
        d = hashlib.md5()
        for buf in iter(partial(f.read, 128), b''):
            d.update(buf)
    return d.hexdigest()



"""Default Directories """

DEFAULT_WSI_DIR = '/NDPI_VAULT/ADRC/'
DEFAULT_PYRAMID_DIR = '/bigdata3/PYRAMIDS/ADRC/'
DEFAULT_DATABASE = 'adrc_slide_database'
DEFAULT_IIP_SERVER_ADDRESS = "http://node15.cci.emory.edu/cgi-bin/iipsrv.fcgi?Zoomify=";

"""CDSA SPECIFIC VARIABLES AND PATHS """
tcga_tumor_types = [ 'acc','blca','blnp','blp','brca','cesc','cntl','coad','dlbc','esca','gbm','hnsc','kich','kirc','kirp','laml','lcll','lcml','lgg','lihc','luad',\
        'lusc','meso','ov','paad','pcpg','prad','read','sarc','skcm','stad','tgct','thca','ucec','ucs','uvm']


PATH_REPORT_ROOT_DIRS = ['/bcr/intgen.org/pathology_reports/reports/','/bcr/nationwidechildrens.org/pathology_reports/reports/']
CLIN_REPORT_ROOT = '/bcr/biotab/clin/'

CLIN_REPORT_ROOT_DIRS  = ['/bcr/biotab/clin/']

dl_dir = "/SYNOLOGY_TCGA_MIRROR/TCGA_LOCAL_MIRROR/"
TCGA_LOCAL_ROOT_DIR = dl_dir + 'tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/'
               
TCGA_HTTP_ROOT_URL = 'https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/'
"""PARAMETERS AND VARIABLE INITIALIZATION """

verbose = 0

default_level = ',0 '   ### default layer to use for ndpi2tiff
ndpi_count = 0
_verbose = 0
_verbose = 1
script_id_num = 3800 ### going to increment from some number...maybe ill make this random later

class LinePrinter():
	"""
	Print things to stdout on one line dynamically
	"""
	def __init__(self,data):
		sys.stdout.write("\r\x1b[K"+data.__str__())
		sys.stdout.flush()

""" 
REGULAR EXPRESSION
"""
parse_tcga_tissue_and_stain_type = re.compile(r'org_(..*)\.(diagnostic|tissue)_images',re.IGNORECASE) 
parse_TCGA_SUBJECT_ID = re.compile(r'(TCGA-..-....)')
parse_full_TCGA_ID = re.compile(r'(TCGA-..-....)-(\d\d)(.)-([^-]*)',re.IGNORECASE)	

adrc_pat_one = re.compile(r'(ADRC\d\d-\d+)_(...?)_(.*)\.ndpi$', re.IGNORECASE)
adrc_pat_two = re.compile(r'(OS\d\d-\d+)_(\d+)_(.+)_(.*)\.ndpi$|(OS\d\d-\d+)_([^_]*)_(.*)\.ndpi$',re.IGNORECASE)
adrc_pat_three = re.compile(r'(E\d\d-\d+)_(\d+)_([^_]+)_(.*)\.ndpi$',re.IGNORECASE)

adrc_dzi_pat_one = re.compile(r'(ADRC\d\d-\d+)_(...?)_(.+)\.ndpi\.dzi\.tif$', re.IGNORECASE)
adrc_dzi_pat_two = re.compile(r'(OS\d\d-\d+)_(\d+)_(.+)_(.*)\.ndpi\.dzi\.tif$|(OS\d\d-\d+)_([^_]*)_(.*)\.ndpi\.dzi\.tif',re.IGNORECASE)
adrc_dzi_pat_three = re.compile(r'(E\d\d-\d+)_(\d?)_(.+)_(.*)\.ndpi\.dzi\.tif$',re.IGNORECASE)

"""
Output files and other logs
"""
f_out = open('corrupt_svs_files.txt','a+')


def connect_to_db( host, user, passwd, db):
	"""I will return two cursors to make my life easier """
	
	try:
		db_dict = MySQLdb.connect(host, user, passwd, db,  cursorclass=MySQLdb.cursors.DictCursor ) 
		db_dict_cursor = db_dict.cursor()
		update_cursor  = db_dict.cursor()
		return( db_dict_cursor, update_cursor)
	except:
		print "Could not connect to the database!!!",host,user,passwd,db
		sys.exit()
		return (None,None)
	
def openslide_test_file(full_file_path,file_type,db_cursor):
	"""This will use the openslide bindings to get the width, height and filesize for an image or return an Error otherwise"""
	width=height=filesize=orig_resolution=slide_title=md5 = None	

	try:
		im = openslide.open_slide(full_file_path)
		(width, height) = im.dimensions
		base_file_name = os.path.basename(full_file_path)
		filesize = os.path.getsize(full_file_path)
		if(file_type== 'svs'):
			orig_resolution = im.properties['aperio.AppMag']
		#md5 = md5Checksum(full_file_path)
		slide_name = os.path.basename(full_file_path)
		return(True,width,height,filesize,orig_resolution,slide_name,md5)	
	except OpenSlideError, e:
		print "Openslide returned an error",full_file_path
		print >>sys.stderr, "Verify failed with:", repr(e.args)
		print "Openslide returned an error",full_file_path
		f_out.write(full_file_path+';\n')
		insert_corrupt_batch_stmt = "insert into `corrupt_or_unreadable_%s_files` (full_file_name,filesize) Values ('%s',%d) "
		print insert_corrupt_batch_stmt % (file_type,full_file_path,os.path.getsize(full_file_path) )
		#update_cursor.execute( insert_corrupt_batch_stmt % (full_file_path,os.path.getsize(full_file_path) ))
		return(False,None,None,None,None,None,None)
	except StandardError, e:
		#file name likely not valid
		print >>sys.stderr, "Verify failed with:", repr(e.args)
		print "Openslide returned an error",full_file_path
		f_out.write(full_file_path+';\n')
		insert_corrupt_batch_stmt = "insert into `corrupt_or_unreadable_%s_files` (full_file_name,filesize) Values ('%s',%d) "
		print insert_corrupt_batch_stmt % (file_type,full_file_path,os.path.getsize(full_file_path) )
		#update_cursor.execute( insert_corrupt_batch_stmt % (full_file_path,os.path.getsize(full_file_path) ))
		return(False,None,None,None,None,None,None)

	except:
		print "failed even earlier on",full_file_path
		"""will log this to a file"""
		return(False,width,height,filesize,orig_resolution,slide_title,md5)	

	return(False,width,height,filesize,orig_resolution,slide_title,md5)	
				
	

def check_image_status_in_db(full_file_path,filetype,db_cursor):
	""" this will do a lookup in the thumb  database and see if the image is already there...
	if it is... I don't bother do any additional file lookups
	some of the metadata extraction can take a bit of time as I need to parse the PNG headers
	filetype can be svs, bigtiff image, ndpi, pyramid image
	"""
	v = _verbose >= 1; vv = _verbose >= 2
	
	if filetype == 'svs':
		sql_lookup = "select count(*) as count from `svs_slide_info` where full_file_path='%s'" %	(full_file_path)
		db_cursor.execute(sql_lookup)
		data = db_cursor.fetchone()
		if data['count'] == 0:
			
			if vv:  print "Need to update entry"
			(valid_image,width,height,filesize,orig_resolution,base_file_name,md5) = openslide_test_file(full_file_path,'svs',db_cursor)
			if valid_image:
				slide_folder = str(full_file_path.split('/')[-2]) 
				sql = "insert into `svs_slide_info` ( slide_filename, image_width,image_height, resolution, full_file_path, slide_folder, filesize ,md5sum ) " 
				sql += " Values ('%s',%s,%s,%s,'%s', '%s',%d,'%s' ) " % ( base_file_name, width, height, orig_resolution, full_file_path, slide_folder, filesize ,md5)
				db_cursor.execute(sql)
	elif filetype == 'pyramid':
		sql_lookup = "select count(*) as count from `dzi_pyramid_info` where full_file_path like ('"+full_file_path+"')" 
		db_cursor.execute(sql_lookup)
		data = db_cursor.fetchone()
		if data['count'] == 0:
			if vv:  print "Need to update entry"
			(valid_image,width,height,filesize,orig_resolution,pyramid_file_name,md5) = openslide_test_file(full_file_path,'pyramid',db_cursor)
			if valid_image:	
				slide_folder = str(full_file_path.split('/')[-2]) 			
				insert_sql = "insert into `dzi_pyramid_info`  ( pyramid_filename, image_width, image_height, full_file_path, file_basename, filesize ,pyramid_folder) "\
				+ " Values ('%s',%d,%d,'%s','%s', %d, '%s' ) " % ( pyramid_file_name, width, height, full_file_path , slide_folder, filesize , slide_folder)
				print insert_sql
				db_cursor.execute(insert_sql)



def set_active_archive_status(metadata_dict_cursor):
	"""This will update and/or set the flag for a slide being an active archive from the TCGA data set"""
	select_stmt = "	select * from `latest_archive_info`"
	print select_stmt
	metadata_dict_cursor.execute(select_stmt)

	result = metadata_dict_cursor.fetchall()
	active_slide_archive = []
	for row in result:
		archive_name = row['ARCHIVE_NAME']
		if 'slide' in archive_name or 'diagnostic' in archive_name or 'tissue' in archive_name:
#			print archive_name
			active_slide_archive.append(archive_name)
	print "I have found",len(active_slide_archive),"active slid archives"
	## i should probably set all rchives to null first..

	####first set the entire thing to not have
	update_stmt = "update svs_slide_info set active_tcga_slide='0'"
	print update_stmt
	metadata_dict_cursor.execute(update_stmt)

	for cur_archive in active_slide_archive:
		update_stmt = "update svs_slide_info set active_tcga_slide='1' where slide_folder='%s'" % cur_archive
		print update_stmt
		metadata_dict_cursor.execute(update_stmt)
	

	"""Now need to check if file is on the filesystem
	result = metadata_dict_cursor.fetchall()
	null_rows = 0
		for row in result:
		full_file_path = row['full_file_path']

		patient_id = 	get_tcga_id( os.path.basename(full_file_path) ,False)
	"""


def validate_slide_pyramid_linkage(db_cursor,db_cursor_two):
	select_stmt = "	select * from `svs_slide_info`"
	db_cursor.execute(select_stmt)
	"""Now need to check if file is on the filesystem"""
	result = db_cursor.fetchall()
	invalid_pyramid_link = 0
	print len(result),"rows to process"
	for row in result:
		#print row
		invalid_row = False
		pyramid = (row['pyramid_filename'])
		if not os.path.isfile(pyramid):
			print "Pyramid is missing...",pyramid
			invalid_row = True
		svs = (row['full_file_path'])
		if not os.path.isfile(svs):
			print "SVS is missing",svs
			invalid_row = True
		if os.path.basename(pyramid).split('.')[0] != os.path.basename(svs).split('.')[0]:
			print svs,pyramid,"DONT SEEM TO MATCH"
			print os.path.basename(pyramid),os.path.basename(svs)
			invalid_row = True
		if invalid_row:
			del_sql =  "delete from svs_slide_info where slide_id='%d'" % row['slide_id']
			db_cursor_two.execute(del_sql)
		##pyramid_file_name and full_file_path


def generate_slide_pyramid_linkage(db_cursor,db_cursor_two):
	""" This will update the slide database and link the pyramids associated with the image.... will scan multiple
	 tables """
	v = _verbose >= 1; vv = _verbose >= 2
	v= True
	vv = True
	
	"""pyramid filenames match on slide_filename in the svs_slide_info table and slide_folder... there are the two 
	main keys"""
	
	""" other fields of import include stain_type and main_project_name... this needs to be duplictable at some point
	since a slide can be in more than one project....   other key field is tissue_type and patient_id
	I may want to have this field iterate multiple fields one by one....
	"""

	## in the dzi_pyramid_info I have two fields that need to be dupdated...parent_slide_title and parent_slide_id
	## probably only need one of these... other field thats relevant is pyramid_folder
	select_stmt = "	select * from `svs_slide_info` where pyramid_generated is NULL"
	db_cursor.execute(select_stmt)
	"""Now need to check if file is on the filesystem"""
	result = db_cursor.fetchall()
	null_rows = 0
	matched_pyramids_found = 0
	for row in result:
		null_rows += 1
		matched_pyramid_file = row['full_file_path'].replace('/bigdata/RAW_SLIDE_LINKS/CDSA/','/bigdata2/PYRAMIDS/CDSA/')+'.dzi.tif'
#		print matched_pyramid_file
		if(os.path.isfile(matched_pyramid_file)):
			update_sql = "update svs_slide_info set pyramid_filename='%s',pyramid_generated='%d' where slide_id='%d'" % (matched_pyramid_file,True,row['slide_id'])
			db_cursor.execute(update_sql)

			matched_pyramids_found += 1
		else:
			pass
		#//there should be a matching pyramid
		#patient_id = 	get_tcga_id( os.path.basename(full_file_path) ,False)
#		print patient_id
#		if not patient_id[0] == None:
#		else:
#			print "Found no patient id...",full_file_path
	print "there were",null_rows,"empty rows and",matched_pyramids_found,"matched pyramids"
	
	select_stmt = "	select * from `svs_slide_info` where patient_id is NULL"

	db_cursor.execute(select_stmt)
	"""Now need to check if file is on the filesystem"""
	result = db_cursor.fetchall()
	null_rows = 0
	for row in result:
		full_file_path = row['full_file_path']
		patient_id = 	get_tcga_id( os.path.basename(full_file_path) ,False)
#		print patient_id
		null_rows += 1
		if not patient_id[0] == None:
			update_sql = "update svs_slide_info set patient_id='%s' where slide_id='%d'" % (patient_id[0],row['slide_id'])
			db_cursor.execute(update_sql)
		else:
			print "Found no patient id...",full_file_path
	print "there were",null_rows,"empty rows"


	select_stmt = "	select * from `svs_slide_info` where stain_type is NULL and tissue_type is NULL"
	db_cursor.execute(select_stmt)
	"""Now need to check if file is on the filesystem"""
	result = db_cursor.fetchall()
	null_rows = 0
	for row in result:
		full_file_path = row['full_file_path']
		(stain_type,tissue_type) = 	get_tcga_stain_type(full_file_path )
		"""I originally AND 'ed the sql statement and it caused it to crash.... i guess that's the logical operator"""
		null_rows += 1
		if not stain_type == None and not tissue_type == None:
			update_sql = "update svs_slide_info set stain_type='%s', tissue_type='%s' where slide_id=%d" %\
			(stain_type,tissue_type,row['slide_id'])
			db_cursor.execute(update_sql)
		else:	
			print "Found no matching group type ...",full_file_path
	print "there were",null_rows,"empty rows"
	select_stmt = "	select * from `dzi_pyramid_info` where parent_slide_id is NULL"
	db_cursor.execute(select_stmt)
	"""Now need to check if file is on the filesystem"""
	result = db_cursor.fetchall()
	null_rows = 0
	for row in result:
		full_file_path = row['full_file_path']
		pyramid_folder = row['pyramid_folder']
		pyramid_filename = row['pyramid_filename'] ### of note it is quite likely the pyramid filename does NOT match the
						## origin slide filename but has extra crap at the end...
						## and also this can be a one to many relationship.. i.e. i may have pyramidized a file
						## multiple times
		pyramid_id = row['pyramid_id']
		slide_filename = pyramid_filename.replace('.dzi.tif','')

		### = row['pyramid_filename'] ### of note it is quite likely the pyramid filename does NOT match the the dzi.tif is the issue
		pyramid_to_orig_slide_match = "select * from svs_slide_info where slide_folder='%s' and slide_filename like '%s'" %(pyramid_folder,slide_filename)

		db_cursor_two.execute(pyramid_to_orig_slide_match)
		slide_match_result = db_cursor_two.fetchall()
		
		if slide_match_result:		
			for slide_row in slide_match_result:
				print slide_row		
				slide_id = slide_row['slide_id']
				"""so now that I found  a match I need to reverse the lookup and get the pyramid id.."""
#			 	set_slide_match_sql =   "update svs_slide_info select * from svs_slide_info where slide_folder='%s' and slide_filename like '%s'" %(pyramid_folder,slide_filename)
				set_pyramid_match_sql = "update dzi_pyramid_info set parent_slide_id='%d' where pyramid_id='%d'"  %(slide_id,pyramid_id)
				db_cursor_two.execute( set_pyramid_match_sql)
		else:
#			print "No match for",slide_filename,"so found a null file set",pyramid_folder
			pass

	"""		null_rows += 1
		if not stain_type == None and not tissue_type == None:
			update_sql = "update svs_slide_info set stain_type='%s', tissue_type='%s' where slide_id=%d" %\
			(stain_type,tissue_type,row['slide_id'])
			metadata_cursor.execute(update_sql)
		else:	
			print "Found no matching group type ...",full_file_path
	print "there were",null_rows,"empty rows"
	"""

def get_file_metadata ( input_file, file_type):
		"""this function wil scan a system file and try axtract certain metadata about the file..
		 this will vary based on the root file type i.e. ndpi, svs, big tff, etc"""
		print input_file, file_type


def find_clin_reports ( tumor_type ):
	"""also grab all the clinical data....."""
	clin_data = []
	clin_data_struct = {}
	""" it seems like the clinical data reports are the cleanest with nationwidechildrens """
	for clin_rpt_dir in CLIN_REPORT_ROOT_DIRS:
		path_base_dir =  TCGA_LOCAL_ROOT_DIR+tumor_type+clin_rpt_dir
		#print path_base_dir
		for dpath, dnames, fnames in os.walk( path_base_dir, followlinks=True):
			for file in fnames:
				if '.txt' in file:
					filebase = file.rstrip('.txt')
					full_file_path = dpath+'/'+filebase
					#full_file_path = 'temp'
					web_path = full_file_path.replace(TCGA_LOCAL_ROOT_DIR,'')
					clin_data_struct[filebase] = { 'web_path':web_path, 'full_file_path':full_file_path }
				
					#Making the full file path a relative web path
					#pdf_path_reports.append(path_data_struct)
	return clin_data_struct


def find_path_reports ( tumor_type ):
	"""this will walk the directories and find pdf files that are path reports """
	pdf_path_reports  = []
	path_data_struct = {}
	"""Path reports seem to be in more than one base directory depending on if intgen or nationwides curated them"""
	
	for PATH_REPORT_ROOT in PATH_REPORT_ROOT_DIRS:
		path_base_dir =  TCGA_LOCAL_ROOT_DIR+tumor_type+PATH_REPORT_ROOT
		#print path_base_dir
		for dpath, dnames, fnames in os.walk( TCGA_LOCAL_ROOT_DIR+tumor_type+PATH_REPORT_ROOT, followlinks=True):
			for file in fnames:
				if '.pdf' in file:
					filebase = file.rstrip('.pdf')
					full_file_path = dpath+'/'+filebase
					#full_file_path = 'temp'
					web_path = full_file_path.replace(TCGA_LOCAL_ROOT_DIR,'')
					path_data_struct[filebase] = { 'web_path':web_path, 'full_file_path':full_file_path }
				
					#Making the full file path a relative web path
					#pdf_path_reports.append(path_data_struct)
	return path_data_struct


def find_tcga_clinical_files ( tumor_type ):
	"""this will walk the directories and find pdf files that are path reports """
	pdf_path_reports  = []
	path_data_struct = {}
	path_base_dir =  TCGA_LOCAL_ROOT_DIR+tumor_type+PATH_REPORT_ROOT
	#print path_base_dir
	for dpath, dnames, fnames in os.walk( TCGA_LOCAL_ROOT_DIR+tumor_type+PATH_REPORT_ROOT, followlinks=True):
		for file in fnames:
			if '.pdf' in file:
				filebase = file.rstrip('.pdf')
				#full_file_path = dpath+'/'+filebase
				full_file_path = 'temp'
				web_path = full_file_path.replace(TCGA_LOCAL_ROOT_DIR,'')
				path_data_struct[filebase] = { 'web_path':web_path, 'full_file_path':full_file_path }
			
				#Making the full file path a relative web path
				#pdf_path_reports.append(path_data_struct)
	return path_data_struct



def find_ndpi_image_list( ndpi_root_path ):	  
	"""project_name is passed along with the potentially more than one root image path for ndpi files"""
	found_ndpi_files = []
	
	ndpi_root_path  = ndpi_root_path.rstrip('/')

	
	for dpath, dnames, fnames in os.walk( ndpi_root_path, followlinks=True):
		for file in fnames:
			if '.ndpi' in file:
				#filebase = file.rstrip('.ndpi')
				#print dpath
				found_ndpi_files.append(dpath +'/'+file)
	print len(found_ndpi_files),"NDPI files were located"
	return found_ndpi_files

def find_svs_image_list( project_name, svs_root_path_list ):	  
	"""project_name is passed along with the potentially more than one root image path for ndpi files"""
	found_svs_files = []
	svs_files_found = 0
	for svs_root_path in svs_root_path_list:
		print svs_root_path
		for dpath, dnames, fnames in os.walk( svs_root_path+project_name, followlinks=True):
			for file in fnames:
				if '.svs' in file:
					filebase = file.rstrip('.svs')
					full_filename = dpath+'/'+file
					#check_image_status_in_db(full_filename,'svs') # change this to add corrupt files and bytes file found
#					found_svs_files.append(filebase)
					found_svs_files.append(full_filename)
					svs_files_found += 1
					output = "Processed: %d svsfiles " % \
									(svs_files_found )
					#corrupt_svs_count, total_gigapixels, total_bytes, old_batch_svs)		   
   		 			LinePrinter(output)
	return(found_svs_files)

def find_pyramid_images( project_name, pyramid_root_dirs):
	## first find the available resolutions... 
	pyramid_images = []
	pyramids_found = 0
	### I am going to add or scan for a 20X, 5X or 40X instead... and use that
	for pyramid_root in pyramid_root_dirs:
		if os.path.isdir(pyramid_root+project_name):
			for dpath, dnames, fnames in os.walk( pyramid_root+project_name, followlinks=True):
				for file in fnames:
					if '.dzi.tif' in file.lower():
						full_filename = dpath+'/'+file
						pyramids_found += 1
						if verbose: print file,dpath	
						#check_image_status_in_db(full_filename,'pyramid') # change this to add corrupt files and bytes file found
						output = "Processed: %d pyramids" % pyramids_found
						LinePrinter(output)
						pyramid_images.append(full_filename)
	return(pyramid_images)

def get_tcga_stain_type( string_to_check):
	""" this function pulls out the stain and tissue type from the TCGA path file names """
	m = parse_tcga_tissue_and_stain_type.search(string_to_check)
	if m:
		return (m.group(1),m.group(2) )
	else:
		return (None,None)

class Table: 
	  def __init__(self, db, name): 
		   self.db = db 
		   self.name = name 
		   self.dbc = self.db.cursor() 

	  def __getitem__(self, item): 
		   self.dbc.execute("select * from %s limit %s, 1" %(self.name, item)) 
		   return self.dbc.fetchone() 
 
	  def __len__(self): 
		   self.dbc.execute("select count(*) as count from %s" % (self.name)) 
		   count_info =  self.dbc.fetchone()
		   l = int( count_info['count'] ) 
		   return l 



"""
Acronyyms and abbreivations used as well as syntax info
 wsi = whole slide image
 -8 specifies bigtiff output and the -c sets the compression
 pick the level to get which should be 0-- i.e. what layer am i trying to convert


"""


def check_for_valid_ADRC_ID( string_to_check):
	"""a file should start with ideally ADRC##-#### or OS or osmething similar
	Valid filename should be ADRCXX-XXXX_<Section>_<STAIN>_<NOtes> """
	m = adrc_pat_one.match(string_to_check)
	m_second_pat = adrc_pat_two.match(string_to_check)
	m_third_pat = adrc_pat_three.match(string_to_check)
	if m:
		patient_id = m.group(1)
		section_id = m.group(2)
		stain = m.group(3)
#		print patient_id,section_id,stain
		return(True)
	elif m_second_pat:
		patient_id = m_second_pat.group(1)
		section_id = m_second_pat.group(2)
		stain  = m_second_pat.group(3)
#		print patient_id,section_id,stain
		return(True)
	elif m_third_pat:
		patient_id = m_third_pat.group(1)
		section_id = m_third_pat.group(2)
		stain  = m_third_pat.group(3)
	else:
		print "no match",string_to_check
		return(False)



def parse_slide_info_for_ADRC_ID( string_to_check):
	"""a file should start with ideally ADRC##-#### or OS or osmething similar
	Valid filename should be ADRCXX-XXXX_<Section>_<STAIN>_<NOtes> """
	stain_tag_normalization_dict = { "AB" : "Abeta", "ABETA" : "ABeta", "US_tau": "Tau", "US_pTDP" : "pTDP",
			"TAU" : "Tau" , "TAU" : "tau", "US_AB" : "ABeta", "US_aSYN-4B12" : "aSyn-4B12",
			"BIEL" : "Biel"}

	m = adrc_dzi_pat_one.match(string_to_check)
	m_second_pat = adrc_dzi_pat_two.match(string_to_check)
	m_third_pat = adrc_dzi_pat_three.match(string_to_check)

	if m:
		patient_id = m.group(1)
		section_id = m.group(2)
		stain = m.group(3)
		if stain in stain_tag_normalization_dict.keys():  stain = stain_tag_normalization_dict[stain]
		print patient_id,section_id,stain
		return(True,patient_id,section_id,stain)
	elif m_second_pat:
		patient_id = m_second_pat.group(1)
		section_id = m_second_pat.group(2)
		stain  = m_second_pat.group(3)
		if stain in stain_tag_normalization_dict.keys():  stain = stain_tag_normalization_dict[stain]
				
		print patient_id,section_id,stain
		return(True,patient_id,section_id,stain)
	elif m_third_pat:
		patient_id = m_third_pat.group(1)
		section_id = m_third_pat.group(2)
		stain  = m_third_pat.group(3)
		if stain in stain_tag_normalization_dict.keys():  stain = stain_tag_normalization_dict[stain]
		print patient_id,section_id,stain
		return(True,patient_id,section_id,stain)

	else:
		print "no match",string_to_check
		return(False,None,None,None)
		

def get_tcga_id( string_to_check , get_full_tcga_id):
		""" will either return the TCGA-12-3456 or the entire TCGA sample ID which is much much longer... TCGA-12-3456-12-23-232-32"""
		if(get_full_tcga_id):
				m = parse_full_TCGA_ID.match(string_to_check)
				if m:
						TCGA_FULL_ID = m.group(1)+'-'+m.group(2)+m.group(3)+'-'+m.group(4)
						return (m.group(1),TCGA_FULL_ID)

				else:
						return None,None
		m = parse_TCGA_SUBJECT_ID.match(string_to_check)
		if m:
				return (m.group(0),'None')
		else:
				return (None,None)



def set_database_slide_metadata(database,table):
	"""this will iterate and update various project related attributes that may not be set on initial parse
	such as stain type, tissue_type , etc... """
	## update stain_Type first
	sql_lookup = "select * from `"+ database + "`.`dzi_pyramid_info` where stain_type is NULL "
	metadata_dict_cursor.execute(sql_lookup)
	data = metadata_dict_cursor.fetchall()
	for row in data:
 #	   	print row
		(found_tags, patient_id, section_id, stain) = parse_slide_info_for_ADRC_ID( row['pyramid_filename'])
		if found_tags:
			update_sql = "update `" + database + "`.`"+"dzi_pyramid_info` set stain_type='%s' where pyramid_id='%d'"  % ( stain, row['pyramid_id'])
			print update_sql
			update_cursor.execute(update_sql)


	update_annotation_sql = "select * from `" + database + "`.`dzi_pyramid_info` where has_annotation is Null"
	metadata_dict_cursor.execute(update_annotation_sql)
	data = metadata_dict_cursor.fetchall()
	for row in data:
		print row						


def update_annotations(database):
	"""will find xml annotation files and update the database """
	base_path = '/var/www/adrc_js/xml_annotation_files/'
	# crawl looking for svs files
	for dirpath, dirnames, filenames in os.walk(base_path, followlinks=True, onerror=_listdir_error):
		for fname in filenames:
			# NDPI (slide) file?
			if 'xml' in fname:
				file_with_path = os.path.join(dirpath, fname)
				print file_with_path,dirpath,dirnames,filenames
				base_filename = os.path.basename(fname)
				base_filename = base_filename.replace('.xml','')
				print base_filename
				find_slide_sql = "select * from dzi_pyramid_info where pyramid_filename like '%s%%'" % (base_filename)
				print find_slide_sql
				metadata_dict_cursor.execute( find_slide_sql)
				data = metadata_dict_cursor.fetchall()
				for row in data:
					print data
					update_sql = "update dzi_pyramid_info set has_annotation='1' where pyramid_id='%d'" % (row['pyramid_id'])
					print update_sql
					update_cursor.execute(update_sql)
					
					
def gen_ndpi_pyramid(input_file,pyramid_file):
	""" this is a new method that will convert an NDPI to a tiff without necessitating tiling"""

	v = _verbose >= 1; vv = _verbose >= 2
	ndpi2tiff_command = "/bigdata3/BIG_TIFF_IMAGES/ndpi2tiff -8 -t -c lzw:2  "

	script_file_base_path = '/fastdata/tmp/SGE_SCRIPTS/'
	SSD_TEMP_SPACE = '/fastdata/tmp/'
	global script_id_num  ### going to increment from some number...maybe ill make this random later
	current_command_list = '#/bin/bash \n'  ### set this to null... ill only open a script file if i actually run a command
	delete_bigtiff_image = True ## determines if I should cleanup/delete the bigtiff i  generate
				## this is an intermediate file before pyramid generation

	print input_file,pyramid_file

	if not os.path.isfile(pyramid_file):
		### for speed I am going to copy the input file to /fastdata/tmp..
		### I am copying the input_file from its home to a cache dir of SSD goodness
		ssd_cached_file = SSD_TEMP_SPACE + os.path.basename(input_file)
		if v: print ssd_cached_file,"cached file name"
		if not os.path.isfile(ssd_cached_file):
			current_command_list += "sleep "+str(random.randint(1,180) ) + ' \n'
			current_command_list += "cp "+input_file+' '+SSD_TEMP_SPACE+'\n'
		## after deliberation copying from the script versus via ssh helps throttle disk copy from
		## the long term image store which is slower..
		## I decided to add a random sleep time of 0 - 180 seconds in each job
		ndpi2tiff_command = ndpi2tiff_command + ssd_cached_file + default_level
		if v: print ndpi2tiff_command
		output_file = ssd_cached_file+',0.tif'
		if not os.path.isfile(output_file):
			current_command_list += ndpi2tiff_command +'\n'

		pyramid_output_dir = os.path.dirname(pyramid_file)
		if not os.path.isdir(pyramid_output_dir):
			os.makedirs(pyramid_output_dir)

		#vips_pyramid_output = cur_file.replace(input_dir,pyramid_directory) +'.dzi.tif'
		vips_command = 'vips im_vips2tiff -v '+output_file+' '+pyramid_file+':jpeg:90,tile:256x256,pyramid,,,,8 '
		print vips_command
		current_command_list += vips_command
		if v: print current_command_list

		### now writing the script
		current_bash_script = script_file_base_path+'ndpi2tiff-'+str(script_id_num)+'.sh'
		f_out = open(current_bash_script,'w')
		f_out.write(current_command_list)
		if delete_bigtiff_image:
			f_out.write('\n rm -rf \''+output_file+'\' \n')
			f_out.write('rm -rf '+ssd_cached_file+' \n')
			## this may be better to just not put part of the command script
		script_id_num += 1
		f_out.close()
		sge_submit_cmd = "qsub -q slide_convert.q "+current_bash_script
		print sge_submit_cmd
		output = subprocess.check_output (sge_submit_cmd,stderr=subprocess.STDOUT, shell=True)
		print output
		


def _listdir_error(error):
	print >>sys.stderr, "Could not traverse/list:", error.filename

def check_files(wsi_dir=DEFAULT_WSI_DIR):
	"""Checks for NDPI and SVS images
		can probably be deleted...
		Arguments:
		wsi_dir -- The base directory to (recursively) search for .ndpi images.
		Returns: counts of found images: (ndpi, pyramid)
	"""
	print "Parsing",wsi_dir
	# sanity checks
	if not os.path.isdir(wsi_dir): 
		raise IOError('SVS or NDPI base path is not a directory or is unreadable: ' + str(wsi_dir))
	# get rid of any trailing slashes
	wsi_dir = wsi_dir.rstrip('/')

	global ndpi_count
	# arg handling
	v = _verbose >= 1; vv = _verbose >= 2
	wsi_prefix_len = len(wsi_dir) + 1 # plus 1 for leading '/'
	ndpi_pat = re.compile(r'.*\.ndpi$', re.IGNORECASE)

	# crawl looking for svs files
	for dirpath, dirnames, filenames in os.walk(wsi_dir, followlinks=True, onerror=_listdir_error):
		for fname in filenames:
			# NDPI (slide) file?
			if ndpi_pat.match(fname):
				ndpi_count +=1
				file_with_path = os.path.join(dirpath, fname)
				if v: print >>sys.stderr, "Slide: ", file_with_path

				path_suffix = dirpath[wsi_prefix_len:]
				path = fname.split('/')
			   	file = path[len(path)-1] 
				### first check if the ndpi file is registered in our database...
				check_image_status_in_db(file_with_path,'ndpi','adrc_slide_database','ndpi_slide_info')

				if check_for_valid_ADRC_ID( file) or  True :
					input_file = os.path.join(dirpath)+'/'+file
					pyramid_file = input_file.replace(DEFAULT_WSI_DIR,DEFAULT_PYRAMID_DIR)+'.dzi.tif'
					if not os.path.isfile(pyramid_file):
						print "Generate pyramid for",file
						gen_ndpi_pyramid(input_file,pyramid_file)
					else:
						check_image_status_in_db(pyramid_file,'pyramid','adrc_slide_database','dzi_pyramid_info')
	return ( ndpi_count)


def create_ADRC_schemas():
	create_adrc_pyramid_schema = """
	CREATE TABLE `dzi_pyramid_info` (
  `pyramid_filename` varchar(200) DEFAULT NULL,
  `image_width` int(10) unsigned DEFAULT NULL,
  `image_height` int(10) unsigned DEFAULT NULL,
  `resolution` int(11) DEFAULT '40',
  `full_file_path` varchar(255) DEFAULT NULL,
  `file_basename` varchar(100) DEFAULT NULL,
  `filesize` int(10) unsigned DEFAULT NULL,
  `parent_slide_filename` varchar(50) DEFAULT NULL,
  `parent_slide_id` int(10) unsigned DEFAULT NULL,
  `pyramid_folder` varchar(80) DEFAULT NULL,
  `main_project_name` varchar(20) DEFAULT NULL,
  `stain_type` varchar(30) DEFAULT NULL,
  `tissue_type` varchar(30) DEFAULT NULL,
  `pyramid_id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`pyramid_id`),
  KEY `full_file_name` (`full_file_path`),
  KEY `full_file_path` (`full_file_path`)
	) ENGINE=MyISAM ;

CREATE TABLE `corrupt_or_unreadable_pyramid_files` (
  `full_file_name` text,
  `filesize` int(10) unsigned DEFAULT NULL,
  `active_archive` tinyint(4) DEFAULT NULL,
  `pyramid_id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`pyramid_id`)
)

	"""	
	print create_adrc_pyramid_schema


"""def main(args=None):
	if args is None: args = sys.argv[1:]
	global _verbose; _verbose = opts.verbose
	currentdir = DEFAULT_WSI_DIR
#	for currentdir in DIRS_WITH_IMAGES:
		#check_files(wsi_dir=opts.wsi_dir)
#	(ndpi_count) = check_files(currentdir+'ADRC61-128/') ## is running on node16
	(ndpi_count) = check_files(currentdir)
#	create_ADRC_schemas()
	#et_database_slide_metadata('adrc_slide_database','dzi_pyramid_info')
#	update_annotations('adrc_slide_databse')
	print "NDPI slides:", ndpi_count
"""

def update_md5_values(database,table_to_crawl,primary_key,db_cursor, update_cursor):
	#sql_lookup = "select * from `%s`.`%s` where md5sum is NULL and pyramid_folder like '%%BRCA%%' " % (database,table_to_crawl)
	sql_lookup = "select * from `%s`.`%s` where md5sum is NULL  " % (database,table_to_crawl)
	db_cursor.execute(sql_lookup)
	data = db_cursor.fetchall()
	print len(data),"rows to process"
	for row in data:
		if os.path.isfile(row['full_file_path']):
		   	print row
		   	update_stmt = "update `%s`.`%s` set md5sum='%s' where %s='%s'" % (database,table_to_crawl,md5sum(row['full_file_path']),primary_key,row[primary_key])
			print update_stmt
			update_cursor.execute(update_stmt)
		else:
			print "missing",row
		   	update_stmt = "delete from  `%s`.`%s` where %s='%s'" % (database,table_to_crawl,primary_key,row[primary_key])
			print update_stmt
			#update_cursor.execute(update_stmt)


def locate_md5_collissions(database,table_to_crawl,db_cursor, update_cursor):
	sql_lookup = "select md5sum, count(*) as count from `%s`.`%s` group by md5sum having count>1" % (database,table_to_crawl)
	print sql_lookup
	db_cursor.execute(sql_lookup)
	data = db_cursor.fetchall()
	print len(data),"rows to process"
	md5_collision_list = []
	for row in data:
	   	#print row
	   	md5_collision_list.append(row['md5sum'])
	#print md5_collision_list
	print len(md5_collision_list),"entries with 2 or more matching md5 values"
	for md5 in md5_collision_list:
		if md5 is not None:
			dup_sql = "select * from `%s`.`%s` where md5sum='%s'" % (database,table_to_crawl,md5)
			#print dup_sql
			db_cursor.execute(dup_sql)
			data = db_cursor.fetchall()
			#print data[0]
			print "------------NEXT ENTRY has %d---------------" % len(data)
			#print data
			filename = os.path.basename(data[0]['full_file_path'])
			
			#print svs_filename
			for row in data:
				print row['pyramid_filename']
				if filename not in row['full_file_path']:
					base_tcga_id = filename.split('.')[0]
					if base_tcga_id not in row['full_file_path']:
						print "shit",filename,row['full_file_path'],base_tcga_id
						print row
#						print data[0]
		#print update_stmt
		#update_cursor.execute(update_stmt)


#pyramid_filename': '/bigdata2/PYRAMIDS/CDSA/BRCA_Diagnostic/nationwidechildrens.org_BRCA.diagnostic_images.Level_1.93.0.0/TCGA-E2-A14Y-01Z-00-DX1.804A22A3-FD8D-4C8A-A766-48D28434DE22.svs.dzi.tif', 'active_tcga_slide': 0, 'resolution': 40L, 'md5sum': None, 'image_width': 113288L, 'pyramid_generated': 1, 'patient_id': 'TCGA-E2-A14Y', 'stain_type': 'BRCA', 'image_height': 84037L, 'filesize': 1971660649L, 'slide_folder': 'nationwidechildrens.org_BRCA.diagnostic_images.Level_1.93.0.0', 'slide_filename': 'TCGA-E2-A14Y-01Z-00-DX1.804A22A3-FD8D-4C8A-A766-48D28434DE22.svs', 'main_project_name': None, 'slide_id': 29602L,
# 'full_file_path': '/bigdata/RAW_SLIDE_LINKS/CDSA-LOCAL/BRCA_Diagnostic/nationwidechildrens.org_BRCA.diagnostic_images.Level_1.93.0.0/TCGA-E2-A14Y-01Z-00-DX1.804A22A3-FD8D-4C8A-A766-48D28434DE22.svs',
# 'tissue_type': 'diagnostic'}

### find collisions across pyramid_filenames as well..

def find_rogue_pyramid_filenames(database,db_cursor,con_two):
	"""so this will check and see if the full file path and the pyramid_filename are... the same file... im wondering if I screwed up at some point
		and made the associations wrong"""
	rogue_sql = "select * from `%s`.`svs_slide_info`" % (database)
	print rogue_sql
	db_cursor.execute(rogue_sql)
	data = db_cursor.fetchall()
	for row in data:
		pyr = os.path.basename( row['pyramid_filename'])
		svs = os.path.basename( row['full_file_path'] )
		if svs not in pyr and pyr is not '':
			print "SHIT, pyr=%s,svs=%s" % ( pyr,svs)
			print row 


def find_unlinked_files( db_cursor):
	"""this will look for archive directories that do not have a corresponding link in the RAW_SLIDE_LINK
	dir"""
	select_stmt = "	select * from `latest_archive_info`"
	print select_stmt
	db_cursor.execute(select_stmt)

	result = db_cursor.fetchall()
	active_slide_archive = []
	for row in result:
		archive_name = row['ARCHIVE_NAME']
		if 'slide' in archive_name or 'diagnostic' in archive_name or 'tissue' in archive_name:
#			print archive_name
			active_slide_archive.append(archive_name)
	print "I have found",len(active_slide_archive),"active slid archives"
	link_path = '/bigdata/RAW_SLIDE_LINKS/CDSA/*/'
	all_linked_dirs = glob.glob( link_path+'*')
	currently_linked_dirs = [ os.path.basename(dir) for dir in all_linked_dirs]
	for active_dir in active_slide_archive:
		if active_dir not in currently_linked_dirs:
			print "need to link",active_dir	

	return(active_slide_archive)

#(cur_one, cur_two) = dsa.connect_to_db('localhost','root','cancersuckz!','cdsa_js_prod')
#import dsa_common_functions as dsa
#(cur_one, cur_two) = dsa.connect_to_db('localhost','root','cancersuckz!','cdsa_js_prod')
#active_archive_list = dsa.find_unlinked_files(cur_one)
#active_archive_list
#history



	"""Now need to check if file is on the filesystem
	result = metadata_dict_cursor.fetchall()
	null_rows = 0
		for row in result:
		full_file_path = row['full_file_path']

		patient_id = 	get_tcga_id( os.path.basename(full_file_path) ,False)
	"""

	
"""
"""

if __name__ == '__main__':
	print "Nothing to do..."	
 	#(con_one,con_two) = connect_to_db('localhost', 'root', 'cancersuckz!', 'cdsa_js_prod')

	find_unlinked_files(con_one)


	#update_md5_values('cdsa_js_prod','svs_slide_info','slide_id',con_one,con_two)
	#locate_md5_collissions('cdsa_js_prod','svs_slide_info',con_one,con_two)
	#locate_md5_collissions('cdsa_js_prod','dzi_pyramid_info',con_one,con_two)
	validate_slide_pyramid_linkage(con_one,con_two)

	#find_rogue_pyramid_filenames('cdsa_js_prod',con_one,con_two)
	
	#update_md5_values('cdsa_js_prod','dzi_pyramid_info','pyramid_id',con_one,con_two)
	generate_slide_pyramid_linkage(con_one,con_two)
