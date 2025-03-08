from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class VendorAddress():

	def __init__(self, VendorAddressId = None, VendorId = None, AddressId = None, AddressTypeId = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'VendorAddress', self.meta,
			Column('VendorAddressId', Integer, primary_key = True),
			Column('VendorId', Integer),
			Column('AddressId', Integer),
			Column('AddressTypeId', Integer),
		)

		self.VendorAddressId = VendorAddressId
		self.VendorId = VendorId
		self.AddressId = AddressId
		self.AddressTypeId = AddressTypeId

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

	def DBFetch(self, VendorAddressId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(VendorAddressId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The VendorAddress is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The VendorAddress is deleted.')

		if self.IsInserted:
			raise InsertError('The VendorAddress is deleted.')

		if self.IsUpdated:
			raise InsertError('The VendorAddress is deleted.')

		if self.VendorId == None:
			raise InsertError('Please make sure that VendorId has a value.')

		if self.AddressId == None:
			raise InsertError('Please make sure that AddressId has a value.')

		if self.AddressTypeId == None:
			raise InsertError('Please make sure that AddressTypeId has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'VendorId':self.VendorId, 'AddressId':self.AddressId, 'AddressTypeId':self.AddressTypeId},
			])
			self.VendorAddressId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The VendorAddress is deleted.')

	def _db_fetch(self, VendorAddressId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.VendorAddressId == self.VendorAddressId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The VendorAddress does not exist. VendorAddress Id is {0}.'.format(str(VendorAddressId)))
			else:
				#Get results and assign them to class variables

				self.VendorAddressId = row[0]
				self.VendorId = row[1]
				self.AddressId = row[2]
				self.AddressTypeId = row[3]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The VendorAddress is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.VendorAddressId == self.VendorAddressId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The VendorAddress is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.VendorAddressId == self.VendorAddressId).values(VendorId = self.VendorId, AddressId = self.AddressId, AddressTypeId = self.AddressTypeId)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

