from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class SetUpBand():

	def __init__(self, SetUpBandId = None, SetUpId = None, BandId = None, Grade = None, Bottom = None, Top = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'SetUpBand', self.meta,
			Column('SetUpBandId', Integer, primary_key = True),
			Column('SetUpId', Integer),
			Column('BandId', Integer),
			Column('Grade', Integer),
			Column('Bottom', Integer),
			Column('Top', Integer),
		)

		self.SetUpBandId = SetUpBandId
		self.SetUpId = SetUpId
		self.BandId = BandId
		self.Grade = Grade
		self.Bottom = Bottom
		self.Top = Top

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

	def DBFetch(self, SetUpBandId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(SetUpBandId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The SetUpBand is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The SetUpBand is deleted.')

		if self.IsInserted:
			raise InsertError('The SetUpBand is deleted.')

		if self.IsUpdated:
			raise InsertError('The SetUpBand is deleted.')

		if self.SetUpId == None:
			raise InsertError('Please make sure that SetUpId has a value.')

		if self.BandId == None:
			raise InsertError('Please make sure that BandId has a value.')

		if self.Grade == None:
			raise InsertError('Please make sure that Grade has a value.')

		if self.Bottom == None:
			raise InsertError('Please make sure that Bottom has a value.')

		if self.Top == None:
			raise InsertError('Please make sure that Top has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'SetUpId':self.SetUpId, 'BandId':self.BandId, 'Grade':self.Grade, 'Bottom':self.Bottom, 'Top':self.Top},
			])
			self.SetUpBandId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The SetUpBand is deleted.')

	def _db_fetch(self, SetUpBandId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.SetUpBandId == self.SetUpBandId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The SetUpBand does not exist. SetUpBand Id is {0}.'.format(str(SetUpBandId)))
			else:
				#Get results and assign them to class variables

				self.SetUpBandId = row[0]
				self.SetUpId = row[1]
				self.BandId = row[2]
				self.Grade = row[3]
				self.Bottom = row[4]
				self.Top = row[5]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The SetUpBand is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.SetUpBandId == self.SetUpBandId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The SetUpBand is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.SetUpBandId == self.SetUpBandId).values(SetUpId = self.SetUpId, BandId = self.BandId, Grade = self.Grade, Bottom = self.Bottom, Top = self.Top)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

