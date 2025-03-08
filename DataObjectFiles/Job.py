from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Job():

	def __init__(self, JobId = None, CompanyId = None, BandId = None, Title = None, Description = None, GradedInd = None, Grade = None, ScoredInd = None, Score = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Job', self.meta,
			Column('JobId', Integer, primary_key = True),
			Column('CompanyId', Integer),
			Column('BandId', Integer),
			Column('Title', String),
			Column('Description', String),
			Column('GradedInd', Integer),
			Column('Grade', Integer),
			Column('ScoredInd', Integer),
			Column('Score', Integer),
		)

		self.JobId = JobId
		self.CompanyId = CompanyId
		self.BandId = BandId
		self.Title = Title
		self.Description = Description
		self.GradedInd = GradedInd
		self.Grade = Grade
		self.ScoredInd = ScoredInd
		self.Score = Score

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

	def DBFetch(self, JobId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(JobId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Job is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Job is deleted.')

		if self.IsInserted:
			raise InsertError('The Job is deleted.')

		if self.IsUpdated:
			raise InsertError('The Job is deleted.')

		if self.CompanyId == None:
			raise InsertError('Please make sure that CompanyId has a value.')

		if self.BandId == None:
			raise InsertError('Please make sure that BandId has a value.')

		if self.Title == None:
			raise InsertError('Please make sure that Title has a value.')

		if self.GradedInd == None:
			raise InsertError('Please make sure that GradedInd has a value.')

		if self.ScoredInd == None:
			raise InsertError('Please make sure that ScoredInd has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'CompanyId':self.CompanyId, 'BandId':self.BandId, 'Title':self.Title, 'Description':self.Description, 'GradedInd':self.GradedInd, 'Grade':self.Grade, 'ScoredInd':self.ScoredInd, 'Score':self.Score},
			])
			self.JobId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Job is deleted.')

	def _db_fetch(self, JobId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.JobId == self.JobId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Job does not exist. Job Id is {0}.'.format(str(JobId)))
			else:
				#Get results and assign them to class variables

				self.JobId = row[0]
				self.CompanyId = row[1]
				self.BandId = row[2]
				self.Title = row[3]
				self.Description = row[4]
				self.GradedInd = row[5]
				self.Grade = row[6]
				self.ScoredInd = row[7]
				self.Score = row[8]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Job is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.JobId == self.JobId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Job is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.JobId == self.JobId).values(CompanyId = self.CompanyId, BandId = self.BandId, Title = self.Title, Description = self.Description, GradedInd = self.GradedInd, Grade = self.Grade, ScoredInd = self.ScoredInd, Score = self.Score)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

