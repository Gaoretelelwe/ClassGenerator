from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class VendorTender():

	def __init__(self, VendorTenderId = None, VendorId = None, TenderId = None, SystemStatusId = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'VendorTender', self.meta,
			Column('VendorTenderId', Integer, primary_key = True),
			Column('VendorId', Integer),
			Column('TenderId', Integer),
			Column('SystemStatusId', Integer),
		)

		self.VendorTenderId = VendorTenderId
		self.VendorId = VendorId
		self.TenderId = TenderId
		self.SystemStatusId = SystemStatusId

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

	def DBFetch(self, VendorTenderId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(VendorTenderId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The VendorTender is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The VendorTender is deleted.')

		if self.IsInserted:
			raise InsertError('The VendorTender is deleted.')

		if self.IsUpdated:
			raise InsertError('The VendorTender is deleted.')

		if self.VendorId == None:
			raise InsertError('Please make sure that VendorId has a value.')

		if self.TenderId == None:
			raise InsertError('Please make sure that TenderId has a value.')

		if self.SystemStatusId == None:
			raise InsertError('Please make sure that SystemStatusId has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'VendorId':self.VendorId, 'TenderId':self.TenderId, 'SystemStatusId':self.SystemStatusId},
			])
			self.VendorTenderId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The VendorTender is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.VendorTenderId == self.VendorTenderId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The VendorTender does not exist. VendorTender Id is {0}.'.format(str(VendorTenderId)))
			else:
				#Get results and assign them to class variables

				self.VendorTenderId = row[0]
				self.VendorId = row[1]
				self.TenderId = row[2]
				self.SystemStatusId = row[3]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The VendorTender is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.VendorTenderId == self.VendorTenderId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The VendorTender is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.VendorTenderId == self.VendorTenderId).values(VendorId = self.VendorId, TenderId = self.TenderId, SystemStatusId = self.SystemStatusId)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

