from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Company():

	def __init__(self, CompanyId = None, ContactPersonId = None, Name = None, RegistrationNumber = None, TaxNumber = None, VATNumber = None, TelephoneNumber = None, MobileNumber = None, EmailAddress = None, Password = None, WebURL = None, RegistrationInd = None, RegistrationDate = None, RegistrationCode = None, ActivationInd = None, ActivationDate = None, ActivateCode = None, Credit = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Company', self.meta,
			Column('CompanyId', Integer, primary_key = True),
			Column('ContactPersonId', Integer),
			Column('Name', String),
			Column('RegistrationNumber', String),
			Column('TaxNumber', String),
			Column('VATNumber', String),
			Column('TelephoneNumber', String),
			Column('MobileNumber', String),
			Column('EmailAddress', String),
			Column('Password', String),
			Column('WebURL', String),
			Column('RegistrationInd', Integer),
			Column('RegistrationDate', DateTime),
			Column('RegistrationCode', String),
			Column('ActivationInd', Integer),
			Column('ActivationDate', DateTime),
			Column('ActivateCode', String),
			Column('Credit', Integer),
		)

		self.CompanyId = CompanyId
		self.ContactPersonId = ContactPersonId
		self.Name = Name
		self.RegistrationNumber = RegistrationNumber
		self.TaxNumber = TaxNumber
		self.VATNumber = VATNumber
		self.TelephoneNumber = TelephoneNumber
		self.MobileNumber = MobileNumber
		self.EmailAddress = EmailAddress
		self.Password = Password
		self.WebURL = WebURL
		self.RegistrationInd = RegistrationInd
		self.RegistrationDate = RegistrationDate
		self.RegistrationCode = RegistrationCode
		self.ActivationInd = ActivationInd
		self.ActivationDate = ActivationDate
		self.ActivateCode = ActivateCode
		self.Credit = Credit

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

	def DBFetch(self, CompanyId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(CompanyId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Company is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Company is deleted.')

		if self.IsInserted:
			raise InsertError('The Company is deleted.')

		if self.IsUpdated:
			raise InsertError('The Company is deleted.')

		if self.ContactPersonId == None:
			raise InsertError('Please make sure that ContactPersonId has a value.')

		if self.Name == None:
			raise InsertError('Please make sure that Name has a value.')

		if self.RegistrationNumber == None:
			raise InsertError('Please make sure that RegistrationNumber has a value.')

		if self.TaxNumber == None:
			raise InsertError('Please make sure that TaxNumber has a value.')

		if self.EmailAddress == None:
			raise InsertError('Please make sure that EmailAddress has a value.')

		if self.Password == None:
			raise InsertError('Please make sure that Password has a value.')

		if self.RegistrationInd == None:
			raise InsertError('Please make sure that RegistrationInd has a value.')

		if self.RegistrationDate == None:
			raise InsertError('Please make sure that RegistrationDate has a value.')

		if self.RegistrationCode == None:
			raise InsertError('Please make sure that RegistrationCode has a value.')

		if self.ActivationInd == None:
			raise InsertError('Please make sure that ActivationInd has a value.')

		if self.ActivateCode == None:
			raise InsertError('Please make sure that ActivateCode has a value.')

		if self.Credit == None:
			raise InsertError('Please make sure that Credit has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'ContactPersonId':self.ContactPersonId, 'Name':self.Name, 'RegistrationNumber':self.RegistrationNumber, 'TaxNumber':self.TaxNumber, 'VATNumber':self.VATNumber, 'TelephoneNumber':self.TelephoneNumber, 'MobileNumber':self.MobileNumber, 'EmailAddress':self.EmailAddress, 'Password':self.Password, 'WebURL':self.WebURL, 'RegistrationInd':self.RegistrationInd, 'RegistrationDate':self.RegistrationDate, 'RegistrationCode':self.RegistrationCode, 'ActivationInd':self.ActivationInd, 'ActivationDate':self.ActivationDate, 'ActivateCode':self.ActivateCode, 'Credit':self.Credit},
			])
			self.CompanyId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Company is deleted.')

	def _db_fetch(self, CompanyId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.CompanyId == self.CompanyId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Company does not exist. Company Id is {0}.'.format(str(CompanyId)))
			else:
				#Get results and assign them to class variables

				self.CompanyId = row[0]
				self.ContactPersonId = row[1]
				self.Name = row[2]
				self.RegistrationNumber = row[3]
				self.TaxNumber = row[4]
				self.VATNumber = row[5]
				self.TelephoneNumber = row[6]
				self.MobileNumber = row[7]
				self.EmailAddress = row[8]
				self.Password = row[9]
				self.WebURL = row[10]
				self.RegistrationInd = row[11]
				self.RegistrationDate = row[12]
				self.RegistrationCode = row[13]
				self.ActivationInd = row[14]
				self.ActivationDate = row[15]
				self.ActivateCode = row[16]
				self.Credit = row[17]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Company is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.CompanyId == self.CompanyId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Company is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.CompanyId == self.CompanyId).values(ContactPersonId = self.ContactPersonId, Name = self.Name, RegistrationNumber = self.RegistrationNumber, TaxNumber = self.TaxNumber, VATNumber = self.VATNumber, TelephoneNumber = self.TelephoneNumber, MobileNumber = self.MobileNumber, EmailAddress = self.EmailAddress, Password = self.Password, WebURL = self.WebURL, RegistrationInd = self.RegistrationInd, RegistrationDate = self.RegistrationDate, RegistrationCode = self.RegistrationCode, ActivationInd = self.ActivationInd, ActivationDate = self.ActivationDate, ActivateCode = self.ActivateCode, Credit = self.Credit)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

