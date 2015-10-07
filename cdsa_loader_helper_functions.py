from openslide.lowlevel import OpenSlideError
import sys,os
import openslide

def clean_openslide_keys ( properties_dict ):
    """Openslide returns dictionaries that have . in the keys which mongo does not like, I need to change this to _"""
    cleaned_dict = {}
    for k,v in properties_dict.iteritems():
        new_key = k.replace('.','_')
        cleaned_dict[new_key] = v
        
    return cleaned_dict

def openslide_test_file_mongo(full_file_path,file_type,db_cursor):
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
                md5 = None
                slide_name = os.path.basename(full_file_path)
                sld_properties = im.properties
                return(True,width,height,filesize,orig_resolution,slide_name,md5,sld_properties)
        except OpenSlideError, e:
                print "Openslide returned an error",full_file_path
                print >>sys.stderr, "Verify failed with:", repr(e.args)
                print "Openslide returned an error",full_file_path
#                f_out.write(full_file_path+';\n')
                print "SHIT IT DIED!"
                
          	
		db_cursor['CDSA_LoadErrors']['corrupt_slides'].insert( { 'full_file_name': full_file_path, 'file_type': file_type, 'filesize': os.path.getsize(full_file_path) } )
                return(False,None,None,None,None,None,None,None)
		      
#                insert_corrupt_batch_stmt = "insert into `corrupt_or_unreadable_%s_files` (full_file_name,filesize) Values ('%s',%d) "
#                print insert_corrupt_batch_stmt % (file_type,full_file_path,os.path.getsize(full_file_path) )
                #update_cursor.execute( insert_corrupt_batch_stmt % (full_file_path,os.path.getsize(full_file_path) ))
                
        except StandardError, e:
                
                #file name likely not valid
                print >>sys.stderr, "Verify failed with:", repr(e.args)
                print "Openslide returned an error om tje StandardError block",full_file_path
                print "SHIT IT DIED!"
		db_cursor['CDSA_LoadErrors']['corrupt_slides'].insert( { 'full_file_name': full_file_path, 'file_type': file_type, 'filesize': os.path.getsize(full_file_path) } )
 
#                sys.exit()
#                f_out.write(full_file_path+';\n')
                insert_corrupt_batch_stmt = "insert into `corrupt_or_unreadable_%s_files` (full_file_name,filesize) Values ('%s',%d) "
                print insert_corrupt_batch_stmt % (file_type,full_file_path,os.path.getsize(full_file_path) )
                
                #update_cursor.execute( insert_corrupt_batch_stmt % (full_file_path,os.path.getsize(full_file_path) ))
                return(False,None,None,None,None,None,None,None)

        except:
                print "failed even earlier on",full_file_path
                """will log this to a file"""
                return(False,width,height,filesize,orig_resolution,slide_title,md5)

        return(False,width,height,filesize,orig_resolution,slide_title,md5)

