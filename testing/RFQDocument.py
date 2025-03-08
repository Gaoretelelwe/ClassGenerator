from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class RFQDocument():

	def __init__(self, RFQDocumentId = None, RFQId = None, DocumentId = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'RFQDocument', self.meta,
			Column('RFQDocumentId', Integer, primary_key = True),
			Column('RFQId', Integer),
			Column('DocumentId', Integer),
		)

		self.RFQDocumentId = RFQDocumentId
		self.RFQId = RFQId
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

	def DBFetch(self, RFQDocumentId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(RFQDocumentId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The RFQDocument is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The RFQDocument is deleted.')

		if self.IsInserted:
			raise InsertError('The RFQDocument is deleted.')

		if self.IsUpdated:
			raise InsertError('The RFQDocument is deleted.')

		if self.RFQId == None:
			raise InsertError('Please make sure that RFQId has a value.')

		if self.DocumentId == None:
			raise InsertError('Please make sure that DocumentId has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'RFQId':self.RFQId, 'DocumentId':self.DocumentId},
			])
			self.RFQDocumentId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The RFQDocument is deleted.')

	def _db_fetch(self, RFQDocumentId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.RFQDocumentId == self.RFQDocumentId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The RFQDocument does not exist. RFQDocument Id is {0}.'.format(str(RFQDocumentId)))
			else:
				#Get results and assign them to class variables

				self.RFQDocumentId = row[0]
				self.RFQId = row[1]
				self.DocumentId = row[2]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The RFQDocument is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.RFQDocumentId == self.RFQDocumentId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The RFQDocument is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.RFQDocumentId == self.RFQDocumentId).values(RFQId = self.RFQId, DocumentId = self.DocumentId)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

