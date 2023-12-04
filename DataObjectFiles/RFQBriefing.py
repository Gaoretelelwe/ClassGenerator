from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class RFQBriefing():

	def __init__(self, RFQBriefingId = None, AddressId = None, BriefingDate = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'RFQBriefing', self.meta,
			Column('RFQBriefingId', Integer, primary_key = True),
			Column('AddressId', Integer),
			Column('BriefingDate', DateTime),
		)

		self.RFQBriefingId = RFQBriefingId
		self.AddressId = AddressId
		self.BriefingDate = BriefingDate

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

	def DBFetch(self, RFQBriefingId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(RFQBriefingId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The RFQBriefing is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The RFQBriefing is deleted.')

		if self.IsInserted:
			raise InsertError('The RFQBriefing is deleted.')

		if self.IsUpdated:
			raise InsertError('The RFQBriefing is deleted.')

		if self.AddressId == None:
			raise InsertError('Please make sure that AddressId has a value.')

		if self.BriefingDate == None:
			raise InsertError('Please make sure that BriefingDate has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'AddressId':self.AddressId, 'BriefingDate':self.BriefingDate},
			])
			self.RFQBriefingId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The RFQBriefing is deleted.')

	def _db_fetch(self, RFQBriefingId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.RFQBriefingId == self.RFQBriefingId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The RFQBriefing does not exist. RFQBriefing Id is {0}.'.format(str(RFQBriefingId)))
			else:
				#Get results and assign them to class variables

				self.RFQBriefingId = row[0]
				self.AddressId = row[1]
				self.BriefingDate = row[2]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The RFQBriefing is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.RFQBriefingId == self.RFQBriefingId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The RFQBriefing is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.RFQBriefingId == self.RFQBriefingId).values(AddressId = self.AddressId, BriefingDate = self.BriefingDate)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

