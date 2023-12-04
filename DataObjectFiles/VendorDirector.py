from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class VendorDirector():

	def __init__(self, VendorDirectorId = None, VendorId = None, PersonId = None, SharePercentage = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'VendorDirector', self.meta,
			Column('VendorDirectorId', Integer, primary_key = True),
			Column('VendorId', Integer),
			Column('PersonId', Integer),
			Column('SharePercentage', String),
		)

		self.VendorDirectorId = VendorDirectorId
		self.VendorId = VendorId
		self.PersonId = PersonId
		self.SharePercentage = SharePercentage

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

	def DBFetch(self, VendorDirectorId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(VendorDirectorId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The VendorDirector is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The VendorDirector is deleted.')

		if self.IsInserted:
			raise InsertError('The VendorDirector is deleted.')

		if self.IsUpdated:
			raise InsertError('The VendorDirector is deleted.')

		if self.VendorId == None:
			raise InsertError('Please make sure that VendorId has a value.')

		if self.PersonId == None:
			raise InsertError('Please make sure that PersonId has a value.')

		if self.SharePercentage == None:
			raise InsertError('Please make sure that SharePercentage has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'VendorId':self.VendorId, 'PersonId':self.PersonId, 'SharePercentage':self.SharePercentage},
			])
			self.VendorDirectorId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The VendorDirector is deleted.')

	def _db_fetch(self, VendorDirectorId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.VendorDirectorId == self.VendorDirectorId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The VendorDirector does not exist. VendorDirector Id is {0}.'.format(str(VendorDirectorId)))
			else:
				#Get results and assign them to class variables

				self.VendorDirectorId = row[0]
				self.VendorId = row[1]
				self.PersonId = row[2]
				self.SharePercentage = row[3]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The VendorDirector is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.VendorDirectorId == self.VendorDirectorId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The VendorDirector is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.VendorDirectorId == self.VendorDirectorId).values(VendorId = self.VendorId, PersonId = self.PersonId, SharePercentage = self.SharePercentage)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

