from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class RFQNotification():

	def __init__(self, RFQNotificationId = None, RFQId = None, EntityId = None, RecepientId = None, SystemStatusId = None, Title = None, Message = None, CreateDate = None, ReadDate = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'RFQNotification', self.meta,
			Column('RFQNotificationId', Integer, primary_key = True),
			Column('RFQId', Integer),
			Column('EntityId', Integer),
			Column('RecepientId', Integer),
			Column('SystemStatusId', Integer),
			Column('Title', String),
			Column('Message', String),
			Column('CreateDate', DateTime),
			Column('ReadDate', DateTime),
		)

		self.RFQNotificationId = RFQNotificationId
		self.RFQId = RFQId
		self.EntityId = EntityId
		self.RecepientId = RecepientId
		self.SystemStatusId = SystemStatusId
		self.Title = Title
		self.Message = Message
		self.CreateDate = CreateDate
		self.ReadDate = ReadDate

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

	def DBFetch(self, RFQNotificationId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(RFQNotificationId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The RFQNotification is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The RFQNotification is deleted.')

		if self.IsInserted:
			raise InsertError('The RFQNotification is deleted.')

		if self.IsUpdated:
			raise InsertError('The RFQNotification is deleted.')

		if self.RFQId == None:
			raise InsertError('Please make sure that RFQId has a value.')

		if self.EntityId == None:
			raise InsertError('Please make sure that EntityId has a value.')

		if self.SystemStatusId == None:
			raise InsertError('Please make sure that SystemStatusId has a value.')

		if self.Message == None:
			raise InsertError('Please make sure that Message has a value.')

		if self.CreateDate == None:
			raise InsertError('Please make sure that CreateDate has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'RFQId':self.RFQId, 'EntityId':self.EntityId, 'RecepientId':self.RecepientId, 'SystemStatusId':self.SystemStatusId, 'Title':self.Title, 'Message':self.Message, 'CreateDate':self.CreateDate, 'ReadDate':self.ReadDate},
			])
			self.RFQNotificationId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The RFQNotification is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.RFQNotificationId == self.RFQNotificationId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The RFQNotification does not exist. RFQNotification Id is {0}.'.format(str(RFQNotificationId)))
			else:
				#Get results and assign them to class variables

				self.RFQNotificationId = row[0]
				self.RFQId = row[1]
				self.EntityId = row[2]
				self.RecepientId = row[3]
				self.SystemStatusId = row[4]
				self.Title = row[5]
				self.Message = row[6]
				self.CreateDate = row[7]
				self.ReadDate = row[8]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The RFQNotification is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.RFQNotificationId == self.RFQNotificationId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The RFQNotification is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.RFQNotificationId == self.RFQNotificationId).values(RFQId = self.RFQId, EntityId = self.EntityId, RecepientId = self.RecepientId, SystemStatusId = self.SystemStatusId, Title = self.Title, Message = self.Message, CreateDate = self.CreateDate, ReadDate = self.ReadDate)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

