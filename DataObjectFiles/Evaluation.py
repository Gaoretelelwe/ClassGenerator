from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Evaluation():

	def __init__(self, EvaluationId = None, BandId = None, DimensionId = None, Level = None, Details = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Evaluation', self.meta,
			Column('EvaluationId', Integer, primary_key = True),
			Column('BandId', Integer),
			Column('DimensionId', Integer),
			Column('Level', Integer),
			Column('Details', String),
		)

		self.EvaluationId = EvaluationId
		self.BandId = BandId
		self.DimensionId = DimensionId
		self.Level = Level
		self.Details = Details

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

	def DBFetch(self, EvaluationId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(EvaluationId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Evaluation is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Evaluation is deleted.')

		if self.IsInserted:
			raise InsertError('The Evaluation is deleted.')

		if self.IsUpdated:
			raise InsertError('The Evaluation is deleted.')

		if self.BandId == None:
			raise InsertError('Please make sure that BandId has a value.')

		if self.DimensionId == None:
			raise InsertError('Please make sure that DimensionId has a value.')

		if self.Level == None:
			raise InsertError('Please make sure that Level has a value.')

		if self.Details == None:
			raise InsertError('Please make sure that Details has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'BandId':self.BandId, 'DimensionId':self.DimensionId, 'Level':self.Level, 'Details':self.Details},
			])
			self.EvaluationId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Evaluation is deleted.')

	def _db_fetch(self, EvaluationId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.EvaluationId == self.EvaluationId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Evaluation does not exist. Evaluation Id is {0}.'.format(str(EvaluationId)))
			else:
				#Get results and assign them to class variables

				self.EvaluationId = row[0]
				self.BandId = row[1]
				self.DimensionId = row[2]
				self.Level = row[3]
				self.Details = row[4]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Evaluation is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.EvaluationId == self.EvaluationId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Evaluation is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.EvaluationId == self.EvaluationId).values(BandId = self.BandId, DimensionId = self.DimensionId, Level = self.Level, Details = self.Details)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

