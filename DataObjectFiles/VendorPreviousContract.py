from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class VendorPreviousContract():

	def __init__(self, VendorPreviousContractId = None, VendorId = None, ClientContactPersonId = None, ClientName = None, EstimatedContractValue = None, YearAwarded = None, YearCompleted = None, DocumentProofAttachedInd = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'VendorPreviousContract', self.meta,
			Column('VendorPreviousContractId', Integer, primary_key = True),
			Column('VendorId', Integer),
			Column('ClientContactPersonId', Integer),
			Column('ClientName', String),
			Column('EstimatedContractValue', String),
			Column('YearAwarded', String),
			Column('YearCompleted', String),
			Column('DocumentProofAttachedInd', Integer),
		)

		self.VendorPreviousContractId = VendorPreviousContractId
		self.VendorId = VendorId
		self.ClientContactPersonId = ClientContactPersonId
		self.ClientName = ClientName
		self.EstimatedContractValue = EstimatedContractValue
		self.YearAwarded = YearAwarded
		self.YearCompleted = YearCompleted
		self.DocumentProofAttachedInd = DocumentProofAttachedInd

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

	def DBFetch(self, VendorPreviousContractId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(VendorPreviousContractId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The VendorPreviousContract is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The VendorPreviousContract is deleted.')

		if self.IsInserted:
			raise InsertError('The VendorPreviousContract is deleted.')

		if self.IsUpdated:
			raise InsertError('The VendorPreviousContract is deleted.')

		if self.VendorId == None:
			raise InsertError('Please make sure that VendorId has a value.')

		if self.ClientContactPersonId == None:
			raise InsertError('Please make sure that ClientContactPersonId has a value.')

		if self.ClientName == None:
			raise InsertError('Please make sure that ClientName has a value.')

		if self.EstimatedContractValue == None:
			raise InsertError('Please make sure that EstimatedContractValue has a value.')

		if self.YearAwarded == None:
			raise InsertError('Please make sure that YearAwarded has a value.')

		if self.DocumentProofAttachedInd == None:
			raise InsertError('Please make sure that DocumentProofAttachedInd has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'VendorId':self.VendorId, 'ClientContactPersonId':self.ClientContactPersonId, 'ClientName':self.ClientName, 'EstimatedContractValue':self.EstimatedContractValue, 'YearAwarded':self.YearAwarded, 'YearCompleted':self.YearCompleted, 'DocumentProofAttachedInd':self.DocumentProofAttachedInd},
			])
			self.VendorPreviousContractId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The VendorPreviousContract is deleted.')

	def _db_fetch(self, VendorPreviousContractId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.VendorPreviousContractId == self.VendorPreviousContractId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The VendorPreviousContract does not exist. VendorPreviousContract Id is {0}.'.format(str(VendorPreviousContractId)))
			else:
				#Get results and assign them to class variables

				self.VendorPreviousContractId = row[0]
				self.VendorId = row[1]
				self.ClientContactPersonId = row[2]
				self.ClientName = row[3]
				self.EstimatedContractValue = row[4]
				self.YearAwarded = row[5]
				self.YearCompleted = row[6]
				self.DocumentProofAttachedInd = row[7]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The VendorPreviousContract is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.VendorPreviousContractId == self.VendorPreviousContractId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The VendorPreviousContract is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.VendorPreviousContractId == self.VendorPreviousContractId).values(VendorId = self.VendorId, ClientContactPersonId = self.ClientContactPersonId, ClientName = self.ClientName, EstimatedContractValue = self.EstimatedContractValue, YearAwarded = self.YearAwarded, YearCompleted = self.YearCompleted, DocumentProofAttachedInd = self.DocumentProofAttachedInd)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

