from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Vendor():

	def __init__(self, VendorId = None, VendorTypeId = None, ContactPersonId = None, Name = None, RegistrationNumber = None, TaxNumber = None, VATNumber = None, TelephoneNumber = None, FaxNumber = None, CellNumber = None, EmailAddress = None, WebURL = None, PreferredContactMethod = None, SystemStatusId = None, RegistrationCode = None, Password = None, ActivationCode = None, PrimeContractorInd = None, SubContractorInd = None, SupplierInd = None, ProfessionalServicesInd = None, ManufacturerInd = None, EducationDevelopmentTrainingInd = None, LabourOnlyContractorInd = None, LabourAgencyInd = None, ConstructionInd = None, OtherInd = None, InformationSuppliedCorrectInd = None, RelevantCopiesAttachedInd = None, PaymentDeliveryRequirementInd = None, VendorCode = None, RegistrationDate = None, ActivatedInd = None, ActivationDate = None, CertificationOfCorrectnessInd = None, CIDBGrade = None, ApprovedInd = None, ApprovedDate = None, ApproverId = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Vendor', self.meta,
			Column('VendorId', Integer, primary_key = True),
			Column('VendorTypeId', Integer),
			Column('ContactPersonId', Integer),
			Column('Name', String),
			Column('RegistrationNumber', String, unique = True),
			Column('TaxNumber', String),
			Column('VATNumber', String),
			Column('TelephoneNumber', String),
			Column('FaxNumber', String),
			Column('CellNumber', String),
			Column('EmailAddress', String, unique = True),
			Column('WebURL', String),
			Column('PreferredContactMethod', String),
			Column('SystemStatusId', Integer),
			Column('RegistrationCode', String),
			Column('Password', String),
			Column('ActivationCode', String),
			Column('PrimeContractorInd', Integer),
			Column('SubContractorInd', Integer),
			Column('SupplierInd', Integer),
			Column('ProfessionalServicesInd', Integer),
			Column('ManufacturerInd', Integer),
			Column('EducationDevelopmentTrainingInd', Integer),
			Column('LabourOnlyContractorInd', Integer),
			Column('LabourAgencyInd', Integer),
			Column('ConstructionInd', Integer),
			Column('OtherInd', Integer),
			Column('InformationSuppliedCorrectInd', Integer),
			Column('RelevantCopiesAttachedInd', Integer),
			Column('PaymentDeliveryRequirementInd', Integer),
			Column('VendorCode', String),
			Column('RegistrationDate', DateTime),
			Column('ActivatedInd', Integer),
			Column('ActivationDate', DateTime),
			Column('CertificationOfCorrectnessInd', Integer),
			Column('CIDBGrade', Integer),
			Column('ApprovedInd', Integer),
			Column('ApprovedDate', DateTime),
			Column('ApproverId', Integer),
		)

		self.VendorId = VendorId
		self.VendorTypeId = VendorTypeId
		self.ContactPersonId = ContactPersonId
		self.Name = Name
		self.RegistrationNumber = RegistrationNumber
		self.TaxNumber = TaxNumber
		self.VATNumber = VATNumber
		self.TelephoneNumber = TelephoneNumber
		self.FaxNumber = FaxNumber
		self.CellNumber = CellNumber
		self.EmailAddress = EmailAddress
		self.WebURL = WebURL
		self.PreferredContactMethod = PreferredContactMethod
		self.SystemStatusId = SystemStatusId
		self.RegistrationCode = RegistrationCode
		self.Password = Password
		self.ActivationCode = ActivationCode
		self.PrimeContractorInd = PrimeContractorInd
		self.SubContractorInd = SubContractorInd
		self.SupplierInd = SupplierInd
		self.ProfessionalServicesInd = ProfessionalServicesInd
		self.ManufacturerInd = ManufacturerInd
		self.EducationDevelopmentTrainingInd = EducationDevelopmentTrainingInd
		self.LabourOnlyContractorInd = LabourOnlyContractorInd
		self.LabourAgencyInd = LabourAgencyInd
		self.ConstructionInd = ConstructionInd
		self.OtherInd = OtherInd
		self.InformationSuppliedCorrectInd = InformationSuppliedCorrectInd
		self.RelevantCopiesAttachedInd = RelevantCopiesAttachedInd
		self.PaymentDeliveryRequirementInd = PaymentDeliveryRequirementInd
		self.VendorCode = VendorCode
		self.RegistrationDate = RegistrationDate
		self.ActivatedInd = ActivatedInd
		self.ActivationDate = ActivationDate
		self.CertificationOfCorrectnessInd = CertificationOfCorrectnessInd
		self.CIDBGrade = CIDBGrade
		self.ApprovedInd = ApprovedInd
		self.ApprovedDate = ApprovedDate
		self.ApproverId = ApproverId

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

	def DBFetch(self, VendorId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(VendorId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Vendor is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Vendor is deleted.')

		if self.IsInserted:
			raise InsertError('The Vendor is deleted.')

		if self.IsUpdated:
			raise InsertError('The Vendor is deleted.')

		if self.VendorTypeId == None:
			raise InsertError('Please make sure that VendorTypeId has a value.')

		if self.Name == None:
			raise InsertError('Please make sure that Name has a value.')

		if self.EmailAddress == None:
			raise InsertError('Please make sure that EmailAddress has a value.')

		if self.SystemStatusId == None:
			raise InsertError('Please make sure that SystemStatusId has a value.')

		if self.Password == None:
			raise InsertError('Please make sure that Password has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'VendorTypeId':self.VendorTypeId, 'ContactPersonId':self.ContactPersonId, 'Name':self.Name, 'RegistrationNumber':self.RegistrationNumber, 'TaxNumber':self.TaxNumber, 'VATNumber':self.VATNumber, 'TelephoneNumber':self.TelephoneNumber, 'FaxNumber':self.FaxNumber, 'CellNumber':self.CellNumber, 'EmailAddress':self.EmailAddress, 'WebURL':self.WebURL, 'PreferredContactMethod':self.PreferredContactMethod, 'SystemStatusId':self.SystemStatusId, 'RegistrationCode':self.RegistrationCode, 'Password':self.Password, 'ActivationCode':self.ActivationCode, 'PrimeContractorInd':self.PrimeContractorInd, 'SubContractorInd':self.SubContractorInd, 'SupplierInd':self.SupplierInd, 'ProfessionalServicesInd':self.ProfessionalServicesInd, 'ManufacturerInd':self.ManufacturerInd, 'EducationDevelopmentTrainingInd':self.EducationDevelopmentTrainingInd, 'LabourOnlyContractorInd':self.LabourOnlyContractorInd, 'LabourAgencyInd':self.LabourAgencyInd, 'ConstructionInd':self.ConstructionInd, 'OtherInd':self.OtherInd, 'InformationSuppliedCorrectInd':self.InformationSuppliedCorrectInd, 'RelevantCopiesAttachedInd':self.RelevantCopiesAttachedInd, 'PaymentDeliveryRequirementInd':self.PaymentDeliveryRequirementInd, 'VendorCode':self.VendorCode, 'RegistrationDate':self.RegistrationDate, 'ActivatedInd':self.ActivatedInd, 'ActivationDate':self.ActivationDate, 'CertificationOfCorrectnessInd':self.CertificationOfCorrectnessInd, 'CIDBGrade':self.CIDBGrade, 'ApprovedInd':self.ApprovedInd, 'ApprovedDate':self.ApprovedDate, 'ApproverId':self.ApproverId},
			])
			self.VendorId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Vendor is deleted.')

	def _db_fetch(self, VendorId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.VendorId == self.VendorId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Vendor does not exist. Vendor Id is {0}.'.format(str(VendorId)))
			else:
				#Get results and assign them to class variables

				self.VendorId = row[0]
				self.VendorTypeId = row[1]
				self.ContactPersonId = row[2]
				self.Name = row[3]
				self.RegistrationNumber = row[4]
				self.TaxNumber = row[5]
				self.VATNumber = row[6]
				self.TelephoneNumber = row[7]
				self.FaxNumber = row[8]
				self.CellNumber = row[9]
				self.EmailAddress = row[10]
				self.WebURL = row[11]
				self.PreferredContactMethod = row[12]
				self.SystemStatusId = row[13]
				self.RegistrationCode = row[14]
				self.Password = row[15]
				self.ActivationCode = row[16]
				self.PrimeContractorInd = row[17]
				self.SubContractorInd = row[18]
				self.SupplierInd = row[19]
				self.ProfessionalServicesInd = row[20]
				self.ManufacturerInd = row[21]
				self.EducationDevelopmentTrainingInd = row[22]
				self.LabourOnlyContractorInd = row[23]
				self.LabourAgencyInd = row[24]
				self.ConstructionInd = row[25]
				self.OtherInd = row[26]
				self.InformationSuppliedCorrectInd = row[27]
				self.RelevantCopiesAttachedInd = row[28]
				self.PaymentDeliveryRequirementInd = row[29]
				self.VendorCode = row[30]
				self.RegistrationDate = row[31]
				self.ActivatedInd = row[32]
				self.ActivationDate = row[33]
				self.CertificationOfCorrectnessInd = row[34]
				self.CIDBGrade = row[35]
				self.ApprovedInd = row[36]
				self.ApprovedDate = row[37]
				self.ApproverId = row[38]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Vendor is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.VendorId == self.VendorId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Vendor is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.VendorId == self.VendorId).values(VendorTypeId = self.VendorTypeId, ContactPersonId = self.ContactPersonId, Name = self.Name, RegistrationNumber = self.RegistrationNumber, TaxNumber = self.TaxNumber, VATNumber = self.VATNumber, TelephoneNumber = self.TelephoneNumber, FaxNumber = self.FaxNumber, CellNumber = self.CellNumber, EmailAddress = self.EmailAddress, WebURL = self.WebURL, PreferredContactMethod = self.PreferredContactMethod, SystemStatusId = self.SystemStatusId, RegistrationCode = self.RegistrationCode, Password = self.Password, ActivationCode = self.ActivationCode, PrimeContractorInd = self.PrimeContractorInd, SubContractorInd = self.SubContractorInd, SupplierInd = self.SupplierInd, ProfessionalServicesInd = self.ProfessionalServicesInd, ManufacturerInd = self.ManufacturerInd, EducationDevelopmentTrainingInd = self.EducationDevelopmentTrainingInd, LabourOnlyContractorInd = self.LabourOnlyContractorInd, LabourAgencyInd = self.LabourAgencyInd, ConstructionInd = self.ConstructionInd, OtherInd = self.OtherInd, InformationSuppliedCorrectInd = self.InformationSuppliedCorrectInd, RelevantCopiesAttachedInd = self.RelevantCopiesAttachedInd, PaymentDeliveryRequirementInd = self.PaymentDeliveryRequirementInd, VendorCode = self.VendorCode, RegistrationDate = self.RegistrationDate, ActivatedInd = self.ActivatedInd, ActivationDate = self.ActivationDate, CertificationOfCorrectnessInd = self.CertificationOfCorrectnessInd, CIDBGrade = self.CIDBGrade, ApprovedInd = self.ApprovedInd, ApprovedDate = self.ApprovedDate, ApproverId = self.ApproverId)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

