from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Document():

	def __init__(self, DocumentId = None, DocumentTypeId = None, Name = None, Directory = None, Filename = None, UploadDate = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Document', self.meta,
			Column('DocumentId', Integer, primary_key = True),
			Column('DocumentTypeId', Integer),
			Column('Name', String),
			Column('Directory', String),
			Column('Filename', String),
			Column('UploadDate', DateTime),
		)

		self.DocumentId = DocumentId
		self.DocumentTypeId = DocumentTypeId
		self.Name = Name
		self.Directory = Directory
		self.Filename = Filename
		self.UploadDate = UploadDate

	def Save(self):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_insert_check()
				self._db_insert()
			else:
				self._db_update_check()
				self._db_update()
		except InsertError as insert_error:
			raise insert_error

	def DBFetch(self, DocumentId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(DocumentId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Document is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Document is deleted.')

		if self.IsInserted:
			raise InsertError('The Document is deleted.')

		if self.IsUpdated:
			raise InsertError('The Document is deleted.')

		if self.DocumentTypeId == None:
			raise InsertError('Please make sure that DocumentTypeId has a value.')

		if self.Name == None:
			raise InsertError('Please make sure that Name has a value.')

		if self.Directory == None:
			raise InsertError('Please make sure that Directory has a value.')

		if self.Filename == None:
			raise InsertError('Please make sure that Filename has a value.')

		if self.UploadDate == None:
			raise InsertError('Please make sure that UploadDate has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'DocumentTypeId':self.DocumentTypeId, 'Name':self.Name, 'Directory':self.Directory, 'Filename':self.Filename, 'UploadDate':self.UploadDate},
			])
			self.DocumentId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Document is deleted.')

	def _db_fetch(self, DocumentId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.DocumentId == self.DocumentId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Document does not exist. Document Id is {0}.'.format(str(DocumentId)))
			else:
				#Get results and assign them to class variables

				self.DocumentId = row[0]
				self.DocumentTypeId = row[1]
				self.Name = row[2]
				self.Directory = row[3]
				self.Filename = row[4]
				self.UploadDate = row[5]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Document is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.DocumentId == self.DocumentId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Document is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.DocumentId == self.DocumentId).values(DocumentTypeId = self.DocumentTypeId, Name = self.Name, Directory = self.Directory, Filename = self.Filename, UploadDate = self.UploadDate)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

