from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class FieldOptionValue():

	def __init__(self, FieldOptionValueId = None, FieldOptionId = None, Order = None, Value = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'FieldOptionValue', self.meta,
			Column('FieldOptionValueId', Integer, primary_key = True),
			Column('FieldOptionId', Integer),
			Column('Order', Integer),
			Column('Value', String),
		)

		self.FieldOptionValueId = FieldOptionValueId
		self.FieldOptionId = FieldOptionId
		self.Order = Order
		self.Value = Value

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

	def DBFetch(self, FieldOptionValueId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(FieldOptionValueId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The FieldOptionValue is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The FieldOptionValue is deleted.')

		if self.IsInserted:
			raise InsertError('The FieldOptionValue is deleted.')

		if self.IsUpdated:
			raise InsertError('The FieldOptionValue is deleted.')

		if self.FieldOptionId == None:
			raise InsertError('Please make sure that FieldOptionId has a value.')

		if self.Value == None:
			raise InsertError('Please make sure that Value has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'FieldOptionId':self.FieldOptionId, 'Order':self.Order, 'Value':self.Value},
			])
			self.FieldOptionValueId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The FieldOptionValue is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.FieldOptionValueId == self.FieldOptionValueId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The FieldOptionValue does not exist. FieldOptionValue Id is {0}.'.format(str(FieldOptionValueId)))
			else:
				#Get results and assign them to class variables

				self.FieldOptionValueId = row[0]
				self.FieldOptionId = row[1]
				self.Order = row[2]
				self.Value = row[3]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The FieldOptionValue is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.FieldOptionValueId == self.FieldOptionValueId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The FieldOptionValue is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.FieldOptionValueId == self.FieldOptionValueId).values(FieldOptionId = self.FieldOptionId, Order = self.Order, Value = self.Value)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

