from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class AdministratorTender():

	def __init__(self, AdministratorTenderId = None, AdministratorId = None, TenderId = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'AdministratorTender', self.meta,
			Column('AdministratorTenderId', Integer, primary_key = True),
			Column('AdministratorId', Integer),
			Column('TenderId', Integer),
		)

		self.AdministratorTenderId = AdministratorTenderId
		self.AdministratorId = AdministratorId
		self.TenderId = TenderId

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

	def DBFetch(self, AdministratorTenderId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(AdministratorTenderId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The AdministratorTender is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The AdministratorTender is deleted.')

		if self.IsInserted:
			raise InsertError('The AdministratorTender is deleted.')

		if self.IsUpdated:
			raise InsertError('The AdministratorTender is deleted.')

		if self.AdministratorId == None:
			raise InsertError('Please make sure that AdministratorId has a value.')

		if self.TenderId == None:
			raise InsertError('Please make sure that TenderId has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'AdministratorId':self.AdministratorId, 'TenderId':self.TenderId},
			])
			self.AdministratorTenderId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The AdministratorTender is deleted.')

	def _db_fetch(self, AdministratorTenderId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.AdministratorTenderId == self.AdministratorTenderId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The AdministratorTender does not exist. AdministratorTender Id is {0}.'.format(str(AdministratorTenderId)))
			else:
				#Get results and assign them to class variables

				self.AdministratorTenderId = row[0]
				self.AdministratorId = row[1]
				self.TenderId = row[2]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The AdministratorTender is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.AdministratorTenderId == self.AdministratorTenderId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The AdministratorTender is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.AdministratorTenderId == self.AdministratorTenderId).values(AdministratorId = self.AdministratorId, TenderId = self.TenderId)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

