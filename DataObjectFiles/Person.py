from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Person():

	def __init__(self, PersonId = None, Firstname = None, Middlename = None, Lastname = None, EmailAddress = None, MobileNumber = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Person', self.meta,
			Column('PersonId', Integer, primary_key = True),
			Column('Firstname', String),
			Column('Middlename', String),
			Column('Lastname', String),
			Column('EmailAddress', String),
			Column('MobileNumber', String),
		)

		self.PersonId = PersonId
		self.Firstname = Firstname
		self.Middlename = Middlename
		self.Lastname = Lastname
		self.EmailAddress = EmailAddress
		self.MobileNumber = MobileNumber

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

	def DBFetch(self, PersonId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(PersonId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Person is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Person is deleted.')

		if self.IsInserted:
			raise InsertError('The Person is deleted.')

		if self.IsUpdated:
			raise InsertError('The Person is deleted.')

		if self.Firstname == None:
			raise InsertError('Please make sure that Firstname has a value.')

		if self.Lastname == None:
			raise InsertError('Please make sure that Lastname has a value.')

		if self.EmailAddress == None:
			raise InsertError('Please make sure that EmailAddress has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'Firstname':self.Firstname, 'Middlename':self.Middlename, 'Lastname':self.Lastname, 'EmailAddress':self.EmailAddress, 'MobileNumber':self.MobileNumber},
			])
			self.PersonId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Person is deleted.')

	def _db_fetch(self, PersonId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.PersonId == self.PersonId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Person does not exist. Person Id is {0}.'.format(str(PersonId)))
			else:
				#Get results and assign them to class variables

				self.PersonId = row[0]
				self.Firstname = row[1]
				self.Middlename = row[2]
				self.Lastname = row[3]
				self.EmailAddress = row[4]
				self.MobileNumber = row[5]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Person is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.PersonId == self.PersonId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Person is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.PersonId == self.PersonId).values(Firstname = self.Firstname, Middlename = self.Middlename, Lastname = self.Lastname, EmailAddress = self.EmailAddress, MobileNumber = self.MobileNumber)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

