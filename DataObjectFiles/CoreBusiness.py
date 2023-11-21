from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class CoreBusiness():

	def __init__(self, CoreBusinessId = None, Name = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'CoreBusiness', self.meta,
			Column('CoreBusinessId', Integer, primary_key = True),
			Column('Name', String),
		)

		self.CoreBusinessId = CoreBusinessId
		self.Name = Name

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

	def DBFetch(self, CoreBusinessId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(CoreBusinessId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The CoreBusiness is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The CoreBusiness is deleted.')

		if self.IsInserted:
			raise InsertError('The CoreBusiness is deleted.')

		if self.IsUpdated:
			raise InsertError('The CoreBusiness is deleted.')

		if self.Name == None:
			raise InsertError('Please make sure that Name has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'Name':self.Name},
			])
			self.CoreBusinessId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The CoreBusiness is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.CoreBusinessId == self.CoreBusinessId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The CoreBusiness does not exist. CoreBusiness Id is {0}.'.format(str(CoreBusinessId)))
			else:
				#Get results and assign them to class variables

				self.CoreBusinessId = row[0]
				self.Name = row[1]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The CoreBusiness is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.CoreBusinessId == self.CoreBusinessId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The CoreBusiness is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.CoreBusinessId == self.CoreBusinessId).values(Name = self.Name)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

