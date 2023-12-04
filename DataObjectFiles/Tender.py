from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Tender():

	def __init__(self, TenderId = None, CommodityId = None, CreatorId = None, SystemStatusId = None, EnquiryPersonId = None, DocumentId = None, Number = None, Name = None, Description = None, CreateDate = None, ClosingDate = None, ExpectedSupplyDate = None, PublishedDate = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Tender', self.meta,
			Column('TenderId', Integer, primary_key = True),
			Column('CommodityId', Integer),
			Column('CreatorId', Integer),
			Column('SystemStatusId', Integer),
			Column('EnquiryPersonId', Integer),
			Column('DocumentId', Integer),
			Column('Number', String),
			Column('Name', String),
			Column('Description', String),
			Column('CreateDate', DateTime),
			Column('ClosingDate', DateTime),
			Column('ExpectedSupplyDate', DateTime),
			Column('PublishedDate', DateTime),
		)

		self.TenderId = TenderId
		self.CommodityId = CommodityId
		self.CreatorId = CreatorId
		self.SystemStatusId = SystemStatusId
		self.EnquiryPersonId = EnquiryPersonId
		self.DocumentId = DocumentId
		self.Number = Number
		self.Name = Name
		self.Description = Description
		self.CreateDate = CreateDate
		self.ClosingDate = ClosingDate
		self.ExpectedSupplyDate = ExpectedSupplyDate
		self.PublishedDate = PublishedDate

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

	def DBFetch(self, TenderId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(TenderId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Tender is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Tender is deleted.')

		if self.IsInserted:
			raise InsertError('The Tender is deleted.')

		if self.IsUpdated:
			raise InsertError('The Tender is deleted.')

		if self.Name == None:
			raise InsertError('Please make sure that Name has a value.')

		if self.Description == None:
			raise InsertError('Please make sure that Description has a value.')

		if self.CreateDate == None:
			raise InsertError('Please make sure that CreateDate has a value.')

		if self.ClosingDate == None:
			raise InsertError('Please make sure that ClosingDate has a value.')

		if self.ExpectedSupplyDate == None:
			raise InsertError('Please make sure that ExpectedSupplyDate has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'CommodityId':self.CommodityId, 'CreatorId':self.CreatorId, 'SystemStatusId':self.SystemStatusId, 'EnquiryPersonId':self.EnquiryPersonId, 'DocumentId':self.DocumentId, 'Number':self.Number, 'Name':self.Name, 'Description':self.Description, 'CreateDate':self.CreateDate, 'ClosingDate':self.ClosingDate, 'ExpectedSupplyDate':self.ExpectedSupplyDate, 'PublishedDate':self.PublishedDate},
			])
			self.TenderId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Tender is deleted.')

	def _db_fetch(self, TenderId):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.TenderId == self.TenderId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Tender does not exist. Tender Id is {0}.'.format(str(TenderId)))
			else:
				#Get results and assign them to class variables

				self.TenderId = row[0]
				self.CommodityId = row[1]
				self.CreatorId = row[2]
				self.SystemStatusId = row[3]
				self.EnquiryPersonId = row[4]
				self.DocumentId = row[5]
				self.Number = row[6]
				self.Name = row[7]
				self.Description = row[8]
				self.CreateDate = row[9]
				self.ClosingDate = row[10]
				self.ExpectedSupplyDate = row[11]
				self.PublishedDate = row[12]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Tender is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.TenderId == self.TenderId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Tender is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.TenderId == self.TenderId).values(CommodityId = self.CommodityId, CreatorId = self.CreatorId, SystemStatusId = self.SystemStatusId, EnquiryPersonId = self.EnquiryPersonId, DocumentId = self.DocumentId, Number = self.Number, Name = self.Name, Description = self.Description, CreateDate = self.CreateDate, ClosingDate = self.ClosingDate, ExpectedSupplyDate = self.ExpectedSupplyDate, PublishedDate = self.PublishedDate)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

