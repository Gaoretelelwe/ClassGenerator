from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class VendorCoreBusiness():

	def __init__(self, VendorCoreBusinessId = None, VendorId = None, CoreBusinessId = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'VendorCoreBusiness', self.meta,
			Column('VendorCoreBusinessId', Integer, primary_key = True),
			Column('VendorId', Integer),
			Column('CoreBusinessId', Integer),
		)

		self.VendorCoreBusinessId = VendorCoreBusinessId
		self.VendorId = VendorId
		self.CoreBusinessId = CoreBusinessId

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

	def DBFetch(self, VendorCoreBusinessId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(VendorCoreBusinessId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The VendorCoreBusiness is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The VendorCoreBusiness is deleted.')

		if self.IsInserted:
			raise InsertError('The VendorCoreBusiness is deleted.')

		if self.IsUpdated:
			raise InsertError('The VendorCoreBusiness is deleted.')

		if self.VendorId == None:
			raise InsertError('Please make sure that VendorId has a value.')

		if self.CoreBusinessId == None:
			raise InsertError('Please make sure that CoreBusinessId has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'VendorId':self.VendorId, 'CoreBusinessId':self.CoreBusinessId},
			])
			self.VendorCoreBusinessId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The VendorCoreBusiness is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.VendorCoreBusinessId == self.VendorCoreBusinessId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The VendorCoreBusiness does not exist. VendorCoreBusiness Id is {0}.'.format(str(VendorCoreBusinessId)))
			else:
				#Get results and assign them to class variables

				self.VendorCoreBusinessId = row[0]
				self.VendorId = row[1]
				self.CoreBusinessId = row[2]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The VendorCoreBusiness is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.VendorCoreBusinessId == self.VendorCoreBusinessId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The VendorCoreBusiness is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.VendorCoreBusinessId == self.VendorCoreBusinessId).values(VendorId = self.VendorId, CoreBusinessId = self.CoreBusinessId)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

