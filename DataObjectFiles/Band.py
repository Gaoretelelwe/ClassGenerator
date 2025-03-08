from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Band():

	def __init__(self, BandId = None, Name = None, Order = None, MinScore = None, MaxScore = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Band', self.meta,
			Column('BandId', Integer, primary_key = True),
			Column('Name', String),
			Column('Order', Integer),
			Column('MinScore', Integer),
			Column('MaxScore', Integer),
		)

		self.BandId = BandId
		self.Name = Name
		self.Order = Order
		self.MinScore = MinScore
		self.MaxScore = MaxScore

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

	def DBFetch(self, BandId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(BandId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Band is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Band is deleted.')

		if self.IsInserted:
			raise InsertError('The Band is deleted.')

		if self.IsUpdated:
			raise InsertError('The Band is deleted.')

		if self.Name == None:
			raise InsertError('Please make sure that Name has a value.')

		if self.MinScore == None:
			raise InsertError('Please make sure that MinScore has a value.')

		if self.MaxScore == None:
			raise InsertError('Please make sure that MaxScore has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'Name':self.Name, 'Order':self.Order, 'MinScore':self.MinScore, 'MaxScore':self.MaxScore},
			])
			self.BandId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Band is deleted.')

	def _db_fetch(self, BandId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.BandId == self.BandId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Band does not exist. Band Id is {0}.'.format(str(BandId)))
			else:
				#Get results and assign them to class variables

				self.BandId = row[0]
				self.Name = row[1]
				self.Order = row[2]
				self.MinScore = row[3]
				self.MaxScore = row[4]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Band is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.BandId == self.BandId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Band is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.BandId == self.BandId).values(Name = self.Name, Order = self.Order, MinScore = self.MinScore, MaxScore = self.MaxScore)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

