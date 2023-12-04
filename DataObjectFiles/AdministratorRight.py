from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class AdministratorRight():

	def __init__(self, AdministratorRightId = None, AdministratorId = None, SystemRightId = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'AdministratorRight', self.meta,
			Column('AdministratorRightId', Integer, primary_key = True),
			Column('AdministratorId', Integer),
			Column('SystemRightId', Integer),
		)

		self.AdministratorRightId = AdministratorRightId
		self.AdministratorId = AdministratorId
		self.SystemRightId = SystemRightId

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

	def DBFetch(self, AdministratorRightId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(AdministratorRightId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The AdministratorRight is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The AdministratorRight is deleted.')

		if self.IsInserted:
			raise InsertError('The AdministratorRight is deleted.')

		if self.IsUpdated:
			raise InsertError('The AdministratorRight is deleted.')

		if self.AdministratorId == None:
			raise InsertError('Please make sure that AdministratorId has a value.')

		if self.SystemRightId == None:
			raise InsertError('Please make sure that SystemRightId has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'AdministratorId':self.AdministratorId, 'SystemRightId':self.SystemRightId},
			])
			self.AdministratorRightId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The AdministratorRight is deleted.')

	def _db_fetch(self, AdministratorRightId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.AdministratorRightId == self.AdministratorRightId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The AdministratorRight does not exist. AdministratorRight Id is {0}.'.format(str(AdministratorRightId)))
			else:
				#Get results and assign them to class variables

				self.AdministratorRightId = row[0]
				self.AdministratorId = row[1]
				self.SystemRightId = row[2]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The AdministratorRight is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.AdministratorRightId == self.AdministratorRightId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The AdministratorRight is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.AdministratorRightId == self.AdministratorRightId).values(AdministratorId = self.AdministratorId, SystemRightId = self.SystemRightId)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

