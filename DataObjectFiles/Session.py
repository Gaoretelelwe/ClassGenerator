from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Session():

	def __init__(self, SessionId = None, SessionGuid = None, RememberInd = None, RegisteredInd = None, RegistrationStatusId = None, LoggedInInd = None, AdminInd = None, AdminTypeId = None, LoggedPersonId = None, LoggedPersonTypeId = None, CreateDate = None, LastAccessDate = None, UniqueLoginDays = None, VendorId = None, VendorContactPersonId = None, VendorPhysicalAddressId = None, VendorPostalAddressId = None, VendorSalesPersonId = None, VendorAccountsPersonId = None, VendorBankAccountId = None, VendorCode = None, LoggedVendorId = None, ShareholderId = None, VendorDocumentId = None, VendorPreviousContractId = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Session', self.meta,
			Column('SessionId', Integer, primary_key = True),
			Column('SessionGuid', String, unique = True),
			Column('RememberInd', Integer),
			Column('RegisteredInd', Integer),
			Column('RegistrationStatusId', Integer),
			Column('LoggedInInd', Integer),
			Column('AdminInd', Integer),
			Column('AdminTypeId', Integer),
			Column('LoggedPersonId', Integer),
			Column('LoggedPersonTypeId', Integer),
			Column('CreateDate', DateTime),
			Column('LastAccessDate', DateTime),
			Column('UniqueLoginDays', Integer),
			Column('VendorId', Integer),
			Column('VendorContactPersonId', Integer),
			Column('VendorPhysicalAddressId', Integer),
			Column('VendorPostalAddressId', Integer),
			Column('VendorSalesPersonId', Integer),
			Column('VendorAccountsPersonId', Integer),
			Column('VendorBankAccountId', Integer),
			Column('VendorCode', String),
			Column('LoggedVendorId', Integer),
			Column('ShareholderId', Integer),
			Column('VendorDocumentId', Integer),
			Column('VendorPreviousContractId', Integer),
		)

		self.SessionId = SessionId
		self.SessionGuid = SessionGuid
		self.RememberInd = RememberInd
		self.RegisteredInd = RegisteredInd
		self.RegistrationStatusId = RegistrationStatusId
		self.LoggedInInd = LoggedInInd
		self.AdminInd = AdminInd
		self.AdminTypeId = AdminTypeId
		self.LoggedPersonId = LoggedPersonId
		self.LoggedPersonTypeId = LoggedPersonTypeId
		self.CreateDate = CreateDate
		self.LastAccessDate = LastAccessDate
		self.UniqueLoginDays = UniqueLoginDays
		self.VendorId = VendorId
		self.VendorContactPersonId = VendorContactPersonId
		self.VendorPhysicalAddressId = VendorPhysicalAddressId
		self.VendorPostalAddressId = VendorPostalAddressId
		self.VendorSalesPersonId = VendorSalesPersonId
		self.VendorAccountsPersonId = VendorAccountsPersonId
		self.VendorBankAccountId = VendorBankAccountId
		self.VendorCode = VendorCode
		self.LoggedVendorId = LoggedVendorId
		self.ShareholderId = ShareholderId
		self.VendorDocumentId = VendorDocumentId
		self.VendorPreviousContractId = VendorPreviousContractId

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

	def DBFetch(self, SessionId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(SessionId)
		except FetchError as fetch_error:
			raise fetch_error

	def DBFetchGuid(self, SessionGuid):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_guid_check()
				self._db_fetch_guid(SessionGuid)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Session is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Session is deleted.')

		if self.IsInserted:
			raise InsertError('The Session is deleted.')

		if self.IsUpdated:
			raise InsertError('The Session is deleted.')

		if self.SessionGuid == None:
			raise InsertError('Please make sure that SessionGuid has a value.')

		if self.RememberInd == None:
			raise InsertError('Please make sure that RememberInd has a value.')

		if self.RegisteredInd == None:
			raise InsertError('Please make sure that RegisteredInd has a value.')

		if self.LoggedInInd == None:
			raise InsertError('Please make sure that LoggedInInd has a value.')

		if self.AdminInd == None:
			raise InsertError('Please make sure that AdminInd has a value.')

		if self.CreateDate == None:
			raise InsertError('Please make sure that CreateDate has a value.')

		if self.LastAccessDate == None:
			raise InsertError('Please make sure that LastAccessDate has a value.')

		if self.UniqueLoginDays == None:
			raise InsertError('Please make sure that UniqueLoginDays has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'SessionGuid':self.SessionGuid, 'RememberInd':self.RememberInd, 'RegisteredInd':self.RegisteredInd, 'RegistrationStatusId':self.RegistrationStatusId, 'LoggedInInd':self.LoggedInInd, 'AdminInd':self.AdminInd, 'AdminTypeId':self.AdminTypeId, 'LoggedPersonId':self.LoggedPersonId, 'LoggedPersonTypeId':self.LoggedPersonTypeId, 'CreateDate':self.CreateDate, 'LastAccessDate':self.LastAccessDate, 'UniqueLoginDays':self.UniqueLoginDays, 'VendorId':self.VendorId, 'VendorContactPersonId':self.VendorContactPersonId, 'VendorPhysicalAddressId':self.VendorPhysicalAddressId, 'VendorPostalAddressId':self.VendorPostalAddressId, 'VendorSalesPersonId':self.VendorSalesPersonId, 'VendorAccountsPersonId':self.VendorAccountsPersonId, 'VendorBankAccountId':self.VendorBankAccountId, 'VendorCode':self.VendorCode, 'LoggedVendorId':self.LoggedVendorId, 'ShareholderId':self.ShareholderId, 'VendorDocumentId':self.VendorDocumentId, 'VendorPreviousContractId':self.VendorPreviousContractId},
			])
			self.SessionId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Session is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.SessionId == self.SessionId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Session does not exist. Session Id is {0}.'.format(str(SessionId)))
			else:
				#Get results and assign them to class variables

				self.SessionId = row[0]
				self.SessionGuid = row[1]
				self.RememberInd = row[2]
				self.RegisteredInd = row[3]
				self.RegistrationStatusId = row[4]
				self.LoggedInInd = row[5]
				self.AdminInd = row[6]
				self.AdminTypeId = row[7]
				self.LoggedPersonId = row[8]
				self.LoggedPersonTypeId = row[9]
				self.CreateDate = row[10]
				self.LastAccessDate = row[11]
				self.UniqueLoginDays = row[12]
				self.VendorId = row[13]
				self.VendorContactPersonId = row[14]
				self.VendorPhysicalAddressId = row[15]
				self.VendorPostalAddressId = row[16]
				self.VendorSalesPersonId = row[17]
				self.VendorAccountsPersonId = row[18]
				self.VendorBankAccountId = row[19]
				self.VendorCode = row[20]
				self.LoggedVendorId = row[21]
				self.ShareholderId = row[22]
				self.VendorDocumentId = row[23]
				self.VendorPreviousContractId = row[24]
				self.IsFetched = True

	def _db_fetch_guid_check(self):
		if self.IsDeleted:
			raise FetchError('The Session is deleted.')

	def _db_fetch_guid(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.SessionGuid == self.SessionGuid)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Session does not exist. Session Guid is {0}.'.format(str(SessionId)))
			else:
				#Get results and assign them to class variables

				self.SessionId = row[0]
				self.SessionGuid = row[1]
				self.RememberInd = row[2]
				self.RegisteredInd = row[3]
				self.RegistrationStatusId = row[4]
				self.LoggedInInd = row[5]
				self.AdminInd = row[6]
				self.AdminTypeId = row[7]
				self.LoggedPersonId = row[8]
				self.LoggedPersonTypeId = row[9]
				self.CreateDate = row[10]
				self.LastAccessDate = row[11]
				self.UniqueLoginDays = row[12]
				self.VendorId = row[13]
				self.VendorContactPersonId = row[14]
				self.VendorPhysicalAddressId = row[15]
				self.VendorPostalAddressId = row[16]
				self.VendorSalesPersonId = row[17]
				self.VendorAccountsPersonId = row[18]
				self.VendorBankAccountId = row[19]
				self.VendorCode = row[20]
				self.LoggedVendorId = row[21]
				self.ShareholderId = row[22]
				self.VendorDocumentId = row[23]
				self.VendorPreviousContractId = row[24]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Session is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.SessionId == self.SessionId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Session is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.SessionId == self.SessionId).values(SessionGuid = self.SessionGuid, RememberInd = self.RememberInd, RegisteredInd = self.RegisteredInd, RegistrationStatusId = self.RegistrationStatusId, LoggedInInd = self.LoggedInInd, AdminInd = self.AdminInd, AdminTypeId = self.AdminTypeId, LoggedPersonId = self.LoggedPersonId, LoggedPersonTypeId = self.LoggedPersonTypeId, CreateDate = self.CreateDate, LastAccessDate = self.LastAccessDate, UniqueLoginDays = self.UniqueLoginDays, VendorId = self.VendorId, VendorContactPersonId = self.VendorContactPersonId, VendorPhysicalAddressId = self.VendorPhysicalAddressId, VendorPostalAddressId = self.VendorPostalAddressId, VendorSalesPersonId = self.VendorSalesPersonId, VendorAccountsPersonId = self.VendorAccountsPersonId, VendorBankAccountId = self.VendorBankAccountId, VendorCode = self.VendorCode, LoggedVendorId = self.LoggedVendorId, ShareholderId = self.ShareholderId, VendorDocumentId = self.VendorDocumentId, VendorPreviousContractId = self.VendorPreviousContractId)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

