#!/usr/bin/env python
#import pyxnat
import os.path as op
import urllib
import re, sys
import tempfile
import shutil
import subprocess as subp
import optparse
import getpass
import gzip

# we only use this to catch exceptions from pyxnat
#import httplib2

class Defaults(object):
	DELETE_NIFTIS = False
	DRY_RUN = False
	#SERVER = 'http://megabrain.cci.emory.edu:8080/xnat_vault'
	SERVER = 'http://xnatview.cci.emory.edu:8080/xnat'
	USER = 'admin'
	TMPDIR = None
	CACHEDIR = op.join(op.expanduser('~'), '.store')
	NIFTI_TAGS = 'base_image'
	NIFTI_RESOURCE = 'NIFTI'
	SCAN_QUALITY = 'usable'

class Keys(object):
	"""Column keys for scan fetching."""
	scan_id = 'xnat:imagescandata/id'
	subject_id = 'xnat:imagesessiondata/subject_id'
	session_id = 'session_ID'
	project_id = 'project'
	scan_type = 'xnat:imagescandata/type'
	scan_quality = 'xnat:imagescandata/quality'

class NiftiLoader(object):
	def __init__(self, server=Defaults.SERVER, user=Defaults.USER, password=None, 
			cachedir=Defaults.CACHEDIR, tmpdir=Defaults.TMPDIR, dryrun=Defaults.DRY_RUN,
			delete_niftis=Defaults.DELETE_NIFTIS):
		"""Creates a new NIFTI loader / generator.
		
		Arguments:
		server -- the xNAT server URL
		user -- the xNAT username
		password -- the xNAT password
		cachedir -- optional, override the pyxnat cache directory
		tmpdir -- optional, create temporary directories for DICOM and NIFTI files in this directory
		dryrun -- dry run, if True don't update the xNAT instance
		delete_niftis -- if NIFTI resources with an unexpected name should be deleted
		"""
		self.iface = pyxnat.Interface(server=server, user=user,
			password=password, cachedir=cachedir)
		self.tmpdir = tmpdir
		self.dryrun = dryrun
		self.delete_niftis = delete_niftis
		
	def execute(self, project, scan_type=None, scan_quality=Defaults.SCAN_QUALITY):
		"""Runs the loader in full.
		
		Arguments:
		project -- the project id
		scan_type -- optional, only scans with the given type
		scan_quality -- optional, only scans with the given quality
		"""
		scans = self.get_scans(project, scan_type=scan_type, scan_quality=scan_quality)
		print len(scans), "scans to process..."
		for scan in scans:
			try:
				self.process_scan(scan)
			except httplib2.HttpLib2Error, e:
				print >>sys.stderr, "HTTP error processing scan:", e.message

	def get_scans(self, project, scan_type=None, scan_quality=Defaults.SCAN_QUALITY):
		"""Fetches scan info for a project.
		
		Arguments:
		project -- the project id
		scan_type -- optional, only scans with the given type
		scan_quality -- optional, only scans with the given quality
		
		Returns:
		a pyxnat JsonTable of results
		"""
		constraints = {}
		if scan_quality:
			constraints[Keys.scan_quality] = scan_quality
		if scan_type:
			constraints[Keys.scan_type] = scan_type
			
		cols = (Keys.project_id, Keys.subject_id, Keys.session_id, Keys.scan_id, Keys.scan_type)
		
		return self.iface.array.scans(project_id=project, experiment_type='xnat:imageSessionData', 
				scan_type='xnat:imageScanData', columns=cols, constraints=constraints)

	def _to_scan_obj(self, row):
		"""Convert JsonTable entry from get_scans() to a pyxnat.core.resources.Scan object"""
		return self.iface.select.project(row[Keys.project_id])\
			.subject(row[Keys.subject_id]) \
			.experiment(row[Keys.session_id]) \
			.scan(row[Keys.scan_id])
			
	def process_scan(self, scan):
		"""Process a particular scan, generating a NIFTI if needed.
		
		Arguments:
		the scan data, should be dict from iterating over the get_scans() result
		"""
		scan_type = scan[Keys.scan_type]
		scan = self._to_scan_obj(scan)
		experiment = scan.parent()
		subject = experiment.parent()
		
		print "Processing:", _scan_str(scan)
		
		nifti_name = '_'.join( x.replace(' ', '-') for x in 
				(scan_type, subject.label(), experiment.label(), 'SCAN' + scan.id()) ) \
				+ '.nii.gz'

		nifti_resource = scan.resource(Defaults.NIFTI_RESOURCE)
		dicom_resource = scan.resource('DICOM')
		
		# check if the resource exists with the expected name
		found = False
		if nifti_resource.exists():
			for nfile in nifti_resource.files():
				if nfile.id() == nifti_name:
					found = True
				elif self.delete_niftis:
					if self.dryrun:
						print "DRY: Would delete NIFTI:", nfile.id()
					else:
						print "Deleting bad NIFTI:", nfile.id()
						nfile.delete()
						
		# already done?
		if found:
			print "Has NIFTI:", _scan_str(scan)
			return True
		
		print "Need NIFTI:", _scan_str(scan)
		if not dicom_resource.exists():
			print >>sys.stderr, "No DICOM for scan", _scan_str(scan)
			return False
		
		dicom_dir = tempfile.mkdtemp(dir=self.tmpdir)
		nifti_dir = tempfile.mkdtemp(dir=self.tmpdir)
		try:
			# download all the DICOM images for this scan
			for dicom_file in dicom_resource.files():
				local_dicom = op.join(dicom_dir, dicom_file.id())
				print "Downloading DICOM to:", local_dicom
				dicom_file.get(local_dicom)
				
			# run the NIFTI generator
			nifti_files = create_nifti(dicom_dir, nifti_dir)
			if not nifti_files:
				print >>sys.stderr, "WARN: mcverter succeeded but no output files were reported."
				return False
			elif len(nifti_files) > 1:
				print >>sys.stderr, "WARN: More than one NIFTI created, skipping update."
				print >>sys.stderr, "Files:", nifti_files
				return False
			
			# upload the result
			nifti_file = gzip_file(nifti_files[0])
			if self.dryrun:
				print "DRY: Would upload", nifti_file, "as", nifti_name
			else:
				print "Uploading NIFTI:", nifti_name
				nifti_resource.file(nifti_name).insert(nifti_file, content=scan_type, tags=Defaults.NIFTI_TAGS, format='NIFTI')
		finally:
			# cleanup temp directories
			shutil.rmtree(dicom_dir)
			shutil.rmtree(nifti_dir)

_mcvert_pat = re.compile(r'Wrote (.+\.nii)', re.IGNORECASE)
def create_nifti(dicom_dir, nifti_dir):
	"""Invokes mcverter to generate NIFTIs from a set of DICOM images.
	
	Arguments:
	dicom_dir -- directory with the DICOM files
	nifti_dir -- output directory for NIFTI files
	
	Raises:
	IOError -- if mcverter fails
	
	Returns:
	a list of paths to the generated NIFTI files
	"""
	nifti_files = []
	
	args = ('mcverter', '-o', nifti_dir, '-d', '-v', '-n', '-f', 'fsl', dicom_dir)
	proc = subp.Popen(args, stdout=subp.PIPE, stderr=subp.PIPE)
	stdout, stderr = proc.communicate()
	if proc.returncode != 0:
		raise IOError(proc.returncode, "mcverter failed.")

	for line in stdout.splitlines():
		match = _mcvert_pat.search(line)
		if match:
			nifti_files.append(match.group(1))

	if not nifti_files:
		print >>sys.stderr, "mcverter produced no files."
		print >>sys.stderr, "mcverter STDOUT:", stdout
		print >>sys.stderr, "mcverter STDERR:", stderr
	
	return nifti_files
	
def gzip_file(infile, outfile=None):
	if outfile is None:
		outfile = infile + '.gz'
	with open(infile, 'rb') as inhandle:
		with gzip.open(outfile, 'wb') as outhandle:
			outhandle.write(inhandle.read())
	return outfile

def _scan_str(scan):
	if not isinstance(scan, pyxnat.core.resources.Scan):
		raise TypeError('Expecting Scan object, not ' + repr(type(scan)))
	exp = scan.parent()
	sub = exp.parent()
	proj = sub.parent()
	return 'Scan(' + proj.id() + '/' + sub.label() + '/' + exp.label() + '/' + scan.id() + ')'

def _parser():
	p = optparse.OptionParser('%prog [OPTIONS] PROJECT')
	
	p.add_option('-s', '--server', dest='server', default=Defaults.SERVER,
		help='The xNAT server URL [default=%s]' % (Defaults.SERVER,))
	
	p.add_option('-u', '--user', dest='user', default=Defaults.USER,
		help='The xNAT username [default=%s]' % (Defaults.USER,))
	
	p.add_option('-p', '--password', dest='password', default=None,
		help='The xNAT password. USE DISCOURAGED, if not given, you will be prompted.')
	
	p.add_option('--scan-type', dest='scan_type', default=None,
		help='Only process scans of the given type. Default is all scan types.')
	
	p.add_option('--quality', dest='scan_quality', default=Defaults.SCAN_QUALITY,
		help=('Only process scans of the given quality ' \
			'(empty string for all qualities). [default=%s]') % (Defaults.SCAN_QUALITY,))
	
	p.add_option('--delete-niftis', dest='delete_niftis', action='store_true', 
		default=Defaults.DELETE_NIFTIS, 
		help=('Delete NIFTI resources that don\'t match the expected name ' \
			'[default=%s]') % (Defaults.DELETE_NIFTIS,))
	
	p.add_option('--dry', dest='dryrun', default=Defaults.DRY_RUN, action='store_true',
		help='Dry run, generate images but don\'t update the xNAT instance.')
	
	p.add_option('--cachedir', dest='cachedir', default=Defaults.CACHEDIR,
		help='Override the pyxat cache location. [default=%s]' % (Defaults.CACHEDIR,))
	
	p.add_option('--tmpdir', dest='tmpdir', default=Defaults.TMPDIR,
		help='Override location of temporary directories for image process.')
	
	return p

def from_opts(opts):
	"""Construct a NiftiLoader from the optparse options."""
	args = {}
	for opt in ('server', 'user', 'password', 'cachedir', 'tmpdir', 'dryrun', 'delete_niftis'):
		if hasattr(opts, opt):
			args[opt] = getattr(opts, opt)
	return NiftiLoader(**args)
	
def main(args=None):
	if args is None: args = sys.argv[1:]
	
	parser = _parser()
	opts, args = parser.parse_args(args)
	
	if not args:
		parser.error('PROJECT is required.')
	if len(args) > 1:
		print >>sys.stderr, "WARN: Ignoring extra arguments:", args[1:]
	
	project = args[0]
	
	# prompt for password
	if opts.password is None:
		if not sys.stdin.isatty():
			parser.error("No password given and not connected to a terminal.")
		opts.password = getpass.getpass()
	
	# run the loader
	loader = from_opts(opts)
	loader.execute(project, scan_type=opts.scan_type, scan_quality=opts.scan_quality)
	
if __name__ == '__main__':
	main()


