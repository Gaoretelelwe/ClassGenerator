from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class VendorRequiredDocumentType():

	def __init__(self, VendorRequiredDocumentTypeId = None, VendorTypeId = None, DocumentTypeId = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'VendorRequiredDocumentType', self.meta,
			Column('VendorRequiredDocumentTypeId', Integer, primary_key = True),
			Column('VendorTypeId', Integer),
			Column('DocumentTypeId', Integer),
		)

		self.VendorRequiredDocumentTypeId = VendorRequiredDocumentTypeId
		self.VendorTypeId = VendorTypeId
		self.DocumentTypeId = DocumentTypeId

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

	def DBFetch(self, VendorRequiredDocumentTypeId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(VendorRequiredDocumentTypeId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The VendorRequiredDocumentType is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The VendorRequiredDocumentType is deleted.')

		if self.IsInserted:
			raise InsertError('The VendorRequiredDocumentType is deleted.')

		if self.IsUpdated:
			raise InsertError('The VendorRequiredDocumentType is deleted.')

		if self.VendorTypeId == None:
			raise InsertError('Please make sure that VendorTypeId has a value.')

		if self.DocumentTypeId == None:
			raise InsertError('Please make sure that DocumentTypeId has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'VendorTypeId':self.VendorTypeId, 'DocumentTypeId':self.DocumentTypeId},
			])
			self.VendorRequiredDocumentTypeId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The VendorRequiredDocumentType is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.VendorRequiredDocumentTypeId == self.VendorRequiredDocumentTypeId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The VendorRequiredDocumentType does not exist. VendorRequiredDocumentType Id is {0}.'.format(str(VendorRequiredDocumentTypeId)))
			else:
				#Get results and assign them to class variables

				self.VendorRequiredDocumentTypeId = row[0]
				self.VendorTypeId = row[1]
				self.DocumentTypeId = row[2]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The VendorRequiredDocumentType is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.VendorRequiredDocumentTypeId == self.VendorRequiredDocumentTypeId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The VendorRequiredDocumentType is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.VendorRequiredDocumentTypeId == self.VendorRequiredDocumentTypeId).values(VendorTypeId = self.VendorTypeId, DocumentTypeId = self.DocumentTypeId)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

