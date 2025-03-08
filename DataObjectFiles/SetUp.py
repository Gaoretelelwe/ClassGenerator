from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class SetUp():

	def __init__(self, SetUpId = None, Name = None, BFGrades = None, AGrades = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'SetUp', self.meta,
			Column('SetUpId', Integer, primary_key = True),
			Column('Name', String),
			Column('BFGrades', Integer),
			Column('AGrades', Integer),
		)

		self.SetUpId = SetUpId
		self.Name = Name
		self.BFGrades = BFGrades
		self.AGrades = AGrades

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

	def DBFetch(self, SetUpId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(SetUpId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The SetUp is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The SetUp is deleted.')

		if self.IsInserted:
			raise InsertError('The SetUp is deleted.')

		if self.IsUpdated:
			raise InsertError('The SetUp is deleted.')

		if self.Name == None:
			raise InsertError('Please make sure that Name has a value.')

		if self.BFGrades == None:
			raise InsertError('Please make sure that BFGrades has a value.')

		if self.AGrades == None:
			raise InsertError('Please make sure that AGrades has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'Name':self.Name, 'BFGrades':self.BFGrades, 'AGrades':self.AGrades},
			])
			self.SetUpId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The SetUp is deleted.')

	def _db_fetch(self, SetUpId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.SetUpId == self.SetUpId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The SetUp does not exist. SetUp Id is {0}.'.format(str(SetUpId)))
			else:
				#Get results and assign them to class variables

				self.SetUpId = row[0]
				self.Name = row[1]
				self.BFGrades = row[2]
				self.AGrades = row[3]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The SetUp is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.SetUpId == self.SetUpId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The SetUp is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.SetUpId == self.SetUpId).values(Name = self.Name, BFGrades = self.BFGrades, AGrades = self.AGrades)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

