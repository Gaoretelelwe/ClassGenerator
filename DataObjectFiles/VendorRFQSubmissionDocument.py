from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class VendorRFQSubmissionDocument():

	def __init__(self, VendorRFQSubmissionDocumentId = None, VendorRFQSubmissionId = None, DocumentId = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'VendorRFQSubmissionDocument', self.meta,
			Column('VendorRFQSubmissionDocumentId', Integer, primary_key = True),
			Column('VendorRFQSubmissionId', Integer),
			Column('DocumentId', Integer),
		)

		self.VendorRFQSubmissionDocumentId = VendorRFQSubmissionDocumentId
		self.VendorRFQSubmissionId = VendorRFQSubmissionId
		self.DocumentId = DocumentId

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

	def DBFetch(self, VendorRFQSubmissionDocumentId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(VendorRFQSubmissionDocumentId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The VendorRFQSubmissionDocument is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The VendorRFQSubmissionDocument is deleted.')

		if self.IsInserted:
			raise InsertError('The VendorRFQSubmissionDocument is deleted.')

		if self.IsUpdated:
			raise InsertError('The VendorRFQSubmissionDocument is deleted.')

		if self.VendorRFQSubmissionId == None:
			raise InsertError('Please make sure that VendorRFQSubmissionId has a value.')

		if self.DocumentId == None:
			raise InsertError('Please make sure that DocumentId has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'VendorRFQSubmissionId':self.VendorRFQSubmissionId, 'DocumentId':self.DocumentId},
			])
			self.VendorRFQSubmissionDocumentId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The VendorRFQSubmissionDocument is deleted.')

	def _db_fetch(self, VendorRFQSubmissionDocumentId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.VendorRFQSubmissionDocumentId == self.VendorRFQSubmissionDocumentId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The VendorRFQSubmissionDocument does not exist. VendorRFQSubmissionDocument Id is {0}.'.format(str(VendorRFQSubmissionDocumentId)))
			else:
				#Get results and assign them to class variables

				self.VendorRFQSubmissionDocumentId = row[0]
				self.VendorRFQSubmissionId = row[1]
				self.DocumentId = row[2]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The VendorRFQSubmissionDocument is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.VendorRFQSubmissionDocumentId == self.VendorRFQSubmissionDocumentId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The VendorRFQSubmissionDocument is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.VendorRFQSubmissionDocumentId == self.VendorRFQSubmissionDocumentId).values(VendorRFQSubmissionId = self.VendorRFQSubmissionId, DocumentId = self.DocumentId)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

