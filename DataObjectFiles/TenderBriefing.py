from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class TenderBriefing():

	def __init__(self, TenderBriefingId = None, AddressId = None, BriefingDate = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'TenderBriefing', self.meta,
			Column('TenderBriefingId', Integer, primary_key = True),
			Column('AddressId', Integer),
			Column('BriefingDate', DateTime),
		)

		self.TenderBriefingId = TenderBriefingId
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

	def DBFetch(self, TenderBriefingId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(TenderBriefingId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The TenderBriefing is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The TenderBriefing is deleted.')

		if self.IsInserted:
			raise InsertError('The TenderBriefing is deleted.')

		if self.IsUpdated:
			raise InsertError('The TenderBriefing is deleted.')

		if self.AddressId == None:
			raise InsertError('Please make sure that AddressId has a value.')

		if self.BriefingDate == None:
			raise InsertError('Please make sure that BriefingDate has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'AddressId':self.AddressId, 'BriefingDate':self.BriefingDate},
			])
			self.TenderBriefingId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The TenderBriefing is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.TenderBriefingId == self.TenderBriefingId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The TenderBriefing does not exist. TenderBriefing Id is {0}.'.format(str(TenderBriefingId)))
			else:
				#Get results and assign them to class variables

				self.TenderBriefingId = row[0]
				self.AddressId = row[1]
				self.BriefingDate = row[2]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The TenderBriefing is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.TenderBriefingId == self.TenderBriefingId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The TenderBriefing is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.TenderBriefingId == self.TenderBriefingId).values(AddressId = self.AddressId, BriefingDate = self.BriefingDate)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

