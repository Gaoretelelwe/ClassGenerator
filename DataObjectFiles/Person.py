from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Person():

	def __init__(self, PersonId = None, PersonTypeId = None, Firstname = None, Middlename = None, Lastname = None, IDNumber = None, PassportNumber = None, EthnicGroup = None, Gender = None, DisabilityInd = None, DailyInvolvedInd = None, TelephoneCode = None, TelephoneNumber = None, MobileNumber = None, EmailAddress = None, Password = None, Title = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Person', self.meta,
			Column('PersonId', Integer, primary_key = True),
			Column('PersonTypeId', Integer),
			Column('Firstname', String),
			Column('Middlename', String),
			Column('Lastname', String),
			Column('IDNumber', String),
			Column('PassportNumber', String),
			Column('EthnicGroup', String),
			Column('Gender', String),
			Column('DisabilityInd', Integer),
			Column('DailyInvolvedInd', Integer),
			Column('TelephoneCode', String),
			Column('TelephoneNumber', String),
			Column('MobileNumber', String),
			Column('EmailAddress', String),
			Column('Password', String),
			Column('Title', String),
		)

		self.PersonId = PersonId
		self.PersonTypeId = PersonTypeId
		self.Firstname = Firstname
		self.Middlename = Middlename
		self.Lastname = Lastname
		self.IDNumber = IDNumber
		self.PassportNumber = PassportNumber
		self.EthnicGroup = EthnicGroup
		self.Gender = Gender
		self.DisabilityInd = DisabilityInd
		self.DailyInvolvedInd = DailyInvolvedInd
		self.TelephoneCode = TelephoneCode
		self.TelephoneNumber = TelephoneNumber
		self.MobileNumber = MobileNumber
		self.EmailAddress = EmailAddress
		self.Password = Password
		self.Title = Title

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

		if self.PersonTypeId == None:
			raise InsertError('Please make sure that PersonTypeId has a value.')

		if self.Firstname == None:
			raise InsertError('Please make sure that Firstname has a value.')

		if self.Lastname == None:
			raise InsertError('Please make sure that Lastname has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'PersonTypeId':self.PersonTypeId, 'Firstname':self.Firstname, 'Middlename':self.Middlename, 'Lastname':self.Lastname, 'IDNumber':self.IDNumber, 'PassportNumber':self.PassportNumber, 'EthnicGroup':self.EthnicGroup, 'Gender':self.Gender, 'DisabilityInd':self.DisabilityInd, 'DailyInvolvedInd':self.DailyInvolvedInd, 'TelephoneCode':self.TelephoneCode, 'TelephoneNumber':self.TelephoneNumber, 'MobileNumber':self.MobileNumber, 'EmailAddress':self.EmailAddress, 'Password':self.Password, 'Title':self.Title},
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
				self.PersonTypeId = row[1]
				self.Firstname = row[2]
				self.Middlename = row[3]
				self.Lastname = row[4]
				self.IDNumber = row[5]
				self.PassportNumber = row[6]
				self.EthnicGroup = row[7]
				self.Gender = row[8]
				self.DisabilityInd = row[9]
				self.DailyInvolvedInd = row[10]
				self.TelephoneCode = row[11]
				self.TelephoneNumber = row[12]
				self.MobileNumber = row[13]
				self.EmailAddress = row[14]
				self.Password = row[15]
				self.Title = row[16]
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
		s = self.content.update().where(self.content.c.PersonId == self.PersonId).values(PersonTypeId = self.PersonTypeId, Firstname = self.Firstname, Middlename = self.Middlename, Lastname = self.Lastname, IDNumber = self.IDNumber, PassportNumber = self.PassportNumber, EthnicGroup = self.EthnicGroup, Gender = self.Gender, DisabilityInd = self.DisabilityInd, DailyInvolvedInd = self.DailyInvolvedInd, TelephoneCode = self.TelephoneCode, TelephoneNumber = self.TelephoneNumber, MobileNumber = self.MobileNumber, EmailAddress = self.EmailAddress, Password = self.Password, Title = self.Title)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

