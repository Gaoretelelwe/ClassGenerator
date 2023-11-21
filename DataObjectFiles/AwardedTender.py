from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class AwardedTender():

	def __init__(self, AwardedTenderId = None, TenderId = None, VendorId = None, DateAwarded = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'AwardedTender', self.meta,
			Column('AwardedTenderId', Integer, primary_key = True),
			Column('TenderId', Integer),
			Column('VendorId', Integer),
			Column('DateAwarded', DateTime),
		)

		self.AwardedTenderId = AwardedTenderId
		self.TenderId = TenderId
		self.VendorId = VendorId
		self.DateAwarded = DateAwarded

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

	def DBFetch(self, AwardedTenderId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(AwardedTenderId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The AwardedTender is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The AwardedTender is deleted.')

		if self.IsInserted:
			raise InsertError('The AwardedTender is deleted.')

		if self.IsUpdated:
			raise InsertError('The AwardedTender is deleted.')

		if self.TenderId == None:
			raise InsertError('Please make sure that TenderId has a value.')

		if self.VendorId == None:
			raise InsertError('Please make sure that VendorId has a value.')

		if self.DateAwarded == None:
			raise InsertError('Please make sure that DateAwarded has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'TenderId':self.TenderId, 'VendorId':self.VendorId, 'DateAwarded':self.DateAwarded},
			])
			self.AwardedTenderId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The AwardedTender is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.AwardedTenderId == self.AwardedTenderId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The AwardedTender does not exist. AwardedTender Id is {0}.'.format(str(AwardedTenderId)))
			else:
				#Get results and assign them to class variables

				self.AwardedTenderId = row[0]
				self.TenderId = row[1]
				self.VendorId = row[2]
				self.DateAwarded = row[3]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The AwardedTender is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.AwardedTenderId == self.AwardedTenderId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The AwardedTender is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.AwardedTenderId == self.AwardedTenderId).values(TenderId = self.TenderId, VendorId = self.VendorId, DateAwarded = self.DateAwarded)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

