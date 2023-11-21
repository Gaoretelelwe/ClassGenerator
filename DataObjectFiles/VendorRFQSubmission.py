from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class VendorRFQSubmission():

	def __init__(self, VendorRFQSubmissionId = None, VendorRFQId = None, SystemStatusId = None, SubmissionDate = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'VendorRFQSubmission', self.meta,
			Column('VendorRFQSubmissionId', Integer, primary_key = True),
			Column('VendorRFQId', Integer),
			Column('SystemStatusId', Integer),
			Column('SubmissionDate', DateTime),
		)

		self.VendorRFQSubmissionId = VendorRFQSubmissionId
		self.VendorRFQId = VendorRFQId
		self.SystemStatusId = SystemStatusId
		self.SubmissionDate = SubmissionDate

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

	def DBFetch(self, VendorRFQSubmissionId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(VendorRFQSubmissionId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The VendorRFQSubmission is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The VendorRFQSubmission is deleted.')

		if self.IsInserted:
			raise InsertError('The VendorRFQSubmission is deleted.')

		if self.IsUpdated:
			raise InsertError('The VendorRFQSubmission is deleted.')

		if self.VendorRFQId == None:
			raise InsertError('Please make sure that VendorRFQId has a value.')

		if self.SystemStatusId == None:
			raise InsertError('Please make sure that SystemStatusId has a value.')

		if self.SubmissionDate == None:
			raise InsertError('Please make sure that SubmissionDate has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'VendorRFQId':self.VendorRFQId, 'SystemStatusId':self.SystemStatusId, 'SubmissionDate':self.SubmissionDate},
			])
			self.VendorRFQSubmissionId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The VendorRFQSubmission is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.VendorRFQSubmissionId == self.VendorRFQSubmissionId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The VendorRFQSubmission does not exist. VendorRFQSubmission Id is {0}.'.format(str(VendorRFQSubmissionId)))
			else:
				#Get results and assign them to class variables

				self.VendorRFQSubmissionId = row[0]
				self.VendorRFQId = row[1]
				self.SystemStatusId = row[2]
				self.SubmissionDate = row[3]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The VendorRFQSubmission is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.VendorRFQSubmissionId == self.VendorRFQSubmissionId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The VendorRFQSubmission is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.VendorRFQSubmissionId == self.VendorRFQSubmissionId).values(VendorRFQId = self.VendorRFQId, SystemStatusId = self.SystemStatusId, SubmissionDate = self.SubmissionDate)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

