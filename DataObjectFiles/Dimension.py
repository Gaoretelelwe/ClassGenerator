from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Dimension():

	def __init__(self, DimensionId = None, Name = None, Code = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Dimension', self.meta,
			Column('DimensionId', Integer, primary_key = True),
			Column('Name', String),
			Column('Code', String),
		)

		self.DimensionId = DimensionId
		self.Name = Name
		self.Code = Code

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

	def DBFetch(self, DimensionId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(DimensionId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Dimension is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Dimension is deleted.')

		if self.IsInserted:
			raise InsertError('The Dimension is deleted.')

		if self.IsUpdated:
			raise InsertError('The Dimension is deleted.')

		if self.Name == None:
			raise InsertError('Please make sure that Name has a value.')

		if self.Code == None:
			raise InsertError('Please make sure that Code has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'Name':self.Name, 'Code':self.Code},
			])
			self.DimensionId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Dimension is deleted.')

	def _db_fetch(self, DimensionId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.DimensionId == self.DimensionId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Dimension does not exist. Dimension Id is {0}.'.format(str(DimensionId)))
			else:
				#Get results and assign them to class variables

				self.DimensionId = row[0]
				self.Name = row[1]
				self.Code = row[2]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Dimension is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.DimensionId == self.DimensionId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Dimension is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.DimensionId == self.DimensionId).values(Name = self.Name, Code = self.Code)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

