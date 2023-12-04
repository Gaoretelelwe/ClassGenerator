from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Administrator():

	def __init__(self, AdministratorId = None, AdministratorTypeId = None, Firstname = None, Lastname = None, Landline = None, EmailAddress = None, Password = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Administrator', self.meta,
			Column('AdministratorId', Integer, primary_key = True),
			Column('AdministratorTypeId', Integer),
			Column('Firstname', String),
			Column('Lastname', String),
			Column('Landline', String),
			Column('EmailAddress', String, unique = True),
			Column('Password', String),
		)

		self.AdministratorId = AdministratorId
		self.AdministratorTypeId = AdministratorTypeId
		self.Firstname = Firstname
		self.Lastname = Lastname
		self.Landline = Landline
		self.EmailAddress = EmailAddress
		self.Password = Password

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

	def DBFetch(self, AdministratorId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(AdministratorId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Administrator is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Administrator is deleted.')

		if self.IsInserted:
			raise InsertError('The Administrator is deleted.')

		if self.IsUpdated:
			raise InsertError('The Administrator is deleted.')

		if self.AdministratorTypeId == None:
			raise InsertError('Please make sure that AdministratorTypeId has a value.')

		if self.Firstname == None:
			raise InsertError('Please make sure that Firstname has a value.')

		if self.Lastname == None:
			raise InsertError('Please make sure that Lastname has a value.')

		if self.EmailAddress == None:
			raise InsertError('Please make sure that EmailAddress has a value.')

		if self.Password == None:
			raise InsertError('Please make sure that Password has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'AdministratorTypeId':self.AdministratorTypeId, 'Firstname':self.Firstname, 'Lastname':self.Lastname, 'Landline':self.Landline, 'EmailAddress':self.EmailAddress, 'Password':self.Password},
			])
			self.AdministratorId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Administrator is deleted.')

	def _db_fetch(self, AdministratorId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.AdministratorId == self.AdministratorId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Administrator does not exist. Administrator Id is {0}.'.format(str(AdministratorId)))
			else:
				#Get results and assign them to class variables

				self.AdministratorId = row[0]
				self.AdministratorTypeId = row[1]
				self.Firstname = row[2]
				self.Lastname = row[3]
				self.Landline = row[4]
				self.EmailAddress = row[5]
				self.Password = row[6]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Administrator is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.AdministratorId == self.AdministratorId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Administrator is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.AdministratorId == self.AdministratorId).values(AdministratorTypeId = self.AdministratorTypeId, Firstname = self.Firstname, Lastname = self.Lastname, Landline = self.Landline, EmailAddress = self.EmailAddress, Password = self.Password)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

