from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class SystemStatus():

	def __init__(self, SystemStatusId = None, StatusTypeId = None, Name = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'SystemStatus', self.meta,
			Column('SystemStatusId', Integer, primary_key = True),
			Column('StatusTypeId', Integer),
			Column('Name', String),
		)

		self.SystemStatusId = SystemStatusId
		self.StatusTypeId = StatusTypeId
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

	def DBFetch(self, SystemStatusId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(SystemStatusId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The SystemStatus is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The SystemStatus is deleted.')

		if self.IsInserted:
			raise InsertError('The SystemStatus is deleted.')

		if self.IsUpdated:
			raise InsertError('The SystemStatus is deleted.')

		if self.StatusTypeId == None:
			raise InsertError('Please make sure that StatusTypeId has a value.')

		if self.Name == None:
			raise InsertError('Please make sure that Name has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'StatusTypeId':self.StatusTypeId, 'Name':self.Name},
			])
			self.SystemStatusId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The SystemStatus is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.SystemStatusId == self.SystemStatusId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The SystemStatus does not exist. SystemStatus Id is {0}.'.format(str(SystemStatusId)))
			else:
				#Get results and assign them to class variables

				self.SystemStatusId = row[0]
				self.StatusTypeId = row[1]
				self.Name = row[2]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The SystemStatus is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.SystemStatusId == self.SystemStatusId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The SystemStatus is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.SystemStatusId == self.SystemStatusId).values(StatusTypeId = self.StatusTypeId, Name = self.Name)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

