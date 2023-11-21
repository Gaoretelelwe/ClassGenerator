from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class BankAccount():

	def __init__(self, BankAccountId = None, BankId = None, AccountTypeId = None, Number = None, AccountHolder = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'BankAccount', self.meta,
			Column('BankAccountId', Integer, primary_key = True),
			Column('BankId', Integer),
			Column('AccountTypeId', Integer),
			Column('Number', String),
			Column('AccountHolder', String),
		)

		self.BankAccountId = BankAccountId
		self.BankId = BankId
		self.AccountTypeId = AccountTypeId
		self.Number = Number
		self.AccountHolder = AccountHolder

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

	def DBFetch(self, BankAccountId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(BankAccountId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The BankAccount is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The BankAccount is deleted.')

		if self.IsInserted:
			raise InsertError('The BankAccount is deleted.')

		if self.IsUpdated:
			raise InsertError('The BankAccount is deleted.')

		if self.BankId == None:
			raise InsertError('Please make sure that BankId has a value.')

		if self.AccountTypeId == None:
			raise InsertError('Please make sure that AccountTypeId has a value.')

		if self.Number == None:
			raise InsertError('Please make sure that Number has a value.')

		if self.AccountHolder == None:
			raise InsertError('Please make sure that AccountHolder has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'BankId':self.BankId, 'AccountTypeId':self.AccountTypeId, 'Number':self.Number, 'AccountHolder':self.AccountHolder},
			])
			self.BankAccountId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The BankAccount is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.BankAccountId == self.BankAccountId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The BankAccount does not exist. BankAccount Id is {0}.'.format(str(BankAccountId)))
			else:
				#Get results and assign them to class variables

				self.BankAccountId = row[0]
				self.BankId = row[1]
				self.AccountTypeId = row[2]
				self.Number = row[3]
				self.AccountHolder = row[4]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The BankAccount is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.BankAccountId == self.BankAccountId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The BankAccount is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.BankAccountId == self.BankAccountId).values(BankId = self.BankId, AccountTypeId = self.AccountTypeId, Number = self.Number, AccountHolder = self.AccountHolder)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

