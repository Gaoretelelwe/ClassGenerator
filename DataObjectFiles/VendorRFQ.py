from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class VendorRFQ():

	def __init__(self, VendorRFQId = None, VendorId = None, RFQId = None, SystemStatusId = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'VendorRFQ', self.meta,
			Column('VendorRFQId', Integer, primary_key = True),
			Column('VendorId', Integer),
			Column('RFQId', Integer),
			Column('SystemStatusId', Integer),
		)

		self.VendorRFQId = VendorRFQId
		self.VendorId = VendorId
		self.RFQId = RFQId
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

	def DBFetch(self, VendorRFQId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(VendorRFQId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The VendorRFQ is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The VendorRFQ is deleted.')

		if self.IsInserted:
			raise InsertError('The VendorRFQ is deleted.')

		if self.IsUpdated:
			raise InsertError('The VendorRFQ is deleted.')

		if self.VendorId == None:
			raise InsertError('Please make sure that VendorId has a value.')

		if self.RFQId == None:
			raise InsertError('Please make sure that RFQId has a value.')

		if self.SystemStatusId == None:
			raise InsertError('Please make sure that SystemStatusId has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'VendorId':self.VendorId, 'RFQId':self.RFQId, 'SystemStatusId':self.SystemStatusId},
			])
			self.VendorRFQId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The VendorRFQ is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.VendorRFQId == self.VendorRFQId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The VendorRFQ does not exist. VendorRFQ Id is {0}.'.format(str(VendorRFQId)))
			else:
				#Get results and assign them to class variables

				self.VendorRFQId = row[0]
				self.VendorId = row[1]
				self.RFQId = row[2]
				self.SystemStatusId = row[3]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The VendorRFQ is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.VendorRFQId == self.VendorRFQId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The VendorRFQ is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.VendorRFQId == self.VendorRFQId).values(VendorId = self.VendorId, RFQId = self.RFQId, SystemStatusId = self.SystemStatusId)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

