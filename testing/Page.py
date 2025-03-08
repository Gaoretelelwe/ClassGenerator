from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Page():

	def __init__(self, PageId = None, Name = None, Location = None, ViewName = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Page', self.meta,
			Column('PageId', Integer, primary_key = True),
			Column('Name', String),
			Column('Location', String),
			Column('ViewName', String),
		)

		self.PageId = PageId
		self.Name = Name
		self.Location = Location
		self.ViewName = ViewName

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

	def DBFetch(self, PageId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(PageId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Page is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Page is deleted.')

		if self.IsInserted:
			raise InsertError('The Page is deleted.')

		if self.IsUpdated:
			raise InsertError('The Page is deleted.')

		if self.Name == None:
			raise InsertError('Please make sure that Name has a value.')

		if self.Location == None:
			raise InsertError('Please make sure that Location has a value.')

		if self.ViewName == None:
			raise InsertError('Please make sure that ViewName has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'Name':self.Name, 'Location':self.Location, 'ViewName':self.ViewName},
			])
			self.PageId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Page is deleted.')

	def _db_fetch(self, PageId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.PageId == self.PageId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Page does not exist. Page Id is {0}.'.format(str(PageId)))
			else:
				#Get results and assign them to class variables

				self.PageId = row[0]
				self.Name = row[1]
				self.Location = row[2]
				self.ViewName = row[3]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Page is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.PageId == self.PageId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Page is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.PageId == self.PageId).values(Name = self.Name, Location = self.Location, ViewName = self.ViewName)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

