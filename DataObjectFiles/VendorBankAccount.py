from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class VendorBankAccount():

	def __init__(self, VendorBankAccountId = None, VendorId = None, BankAccountId = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'VendorBankAccount', self.meta,
			Column('VendorBankAccountId', Integer, primary_key = True),
			Column('VendorId', Integer),
			Column('BankAccountId', Integer),
		)

		self.VendorBankAccountId = VendorBankAccountId
		self.VendorId = VendorId
		self.BankAccountId = BankAccountId

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

	def DBFetch(self, VendorBankAccountId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(VendorBankAccountId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The VendorBankAccount is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The VendorBankAccount is deleted.')

		if self.IsInserted:
			raise InsertError('The VendorBankAccount is deleted.')

		if self.IsUpdated:
			raise InsertError('The VendorBankAccount is deleted.')

		if self.VendorId == None:
			raise InsertError('Please make sure that VendorId has a value.')

		if self.BankAccountId == None:
			raise InsertError('Please make sure that BankAccountId has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'VendorId':self.VendorId, 'BankAccountId':self.BankAccountId},
			])
			self.VendorBankAccountId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The VendorBankAccount is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.VendorBankAccountId == self.VendorBankAccountId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The VendorBankAccount does not exist. VendorBankAccount Id is {0}.'.format(str(VendorBankAccountId)))
			else:
				#Get results and assign them to class variables

				self.VendorBankAccountId = row[0]
				self.VendorId = row[1]
				self.BankAccountId = row[2]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The VendorBankAccount is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.VendorBankAccountId == self.VendorBankAccountId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The VendorBankAccount is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.VendorBankAccountId == self.VendorBankAccountId).values(VendorId = self.VendorId, BankAccountId = self.BankAccountId)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

