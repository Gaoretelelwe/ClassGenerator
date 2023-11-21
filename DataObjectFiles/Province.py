from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Province():

	def __init__(self, ProvinceId = None, CountryId = None, Name = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Province', self.meta,
			Column('ProvinceId', Integer, primary_key = True),
			Column('CountryId', Integer),
			Column('Name', String),
		)

		self.ProvinceId = ProvinceId
		self.CountryId = CountryId
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

	def DBFetch(self, ProvinceId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(ProvinceId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Province is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Province is deleted.')

		if self.IsInserted:
			raise InsertError('The Province is deleted.')

		if self.IsUpdated:
			raise InsertError('The Province is deleted.')

		if self.CountryId == None:
			raise InsertError('Please make sure that CountryId has a value.')

		if self.Name == None:
			raise InsertError('Please make sure that Name has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'CountryId':self.CountryId, 'Name':self.Name},
			])
			self.ProvinceId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Province is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.ProvinceId == self.ProvinceId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Province does not exist. Province Id is {0}.'.format(str(ProvinceId)))
			else:
				#Get results and assign them to class variables

				self.ProvinceId = row[0]
				self.CountryId = row[1]
				self.Name = row[2]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Province is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.ProvinceId == self.ProvinceId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Province is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.ProvinceId == self.ProvinceId).values(CountryId = self.CountryId, Name = self.Name)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

