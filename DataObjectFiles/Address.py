from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class Address():

	def __init__(self, AddressId = None, ProvinceId = None, Line1 = None, Line2 = None, Suburb = None, City = None, WardNumber = None, Postcode = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'Address', self.meta,
			Column('AddressId', Integer, primary_key = True),
			Column('ProvinceId', Integer),
			Column('Line1', String),
			Column('Line2', String),
			Column('Suburb', String),
			Column('City', String),
			Column('WardNumber', Integer),
			Column('Postcode', String),
		)

		self.AddressId = AddressId
		self.ProvinceId = ProvinceId
		self.Line1 = Line1
		self.Line2 = Line2
		self.Suburb = Suburb
		self.City = City
		self.WardNumber = WardNumber
		self.Postcode = Postcode

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

	def DBFetch(self, AddressId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(AddressId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The Address is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The Address is deleted.')

		if self.IsInserted:
			raise InsertError('The Address is deleted.')

		if self.IsUpdated:
			raise InsertError('The Address is deleted.')

		if self.ProvinceId == None:
			raise InsertError('Please make sure that ProvinceId has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'ProvinceId':self.ProvinceId, 'Line1':self.Line1, 'Line2':self.Line2, 'Suburb':self.Suburb, 'City':self.City, 'WardNumber':self.WardNumber, 'Postcode':self.Postcode},
			])
			self.AddressId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The Address is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.AddressId == self.AddressId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The Address does not exist. Address Id is {0}.'.format(str(AddressId)))
			else:
				#Get results and assign them to class variables

				self.AddressId = row[0]
				self.ProvinceId = row[1]
				self.Line1 = row[2]
				self.Line2 = row[3]
				self.Suburb = row[4]
				self.City = row[5]
				self.WardNumber = row[6]
				self.Postcode = row[7]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The Address is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.AddressId == self.AddressId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The Address is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.AddressId == self.AddressId).values(ProvinceId = self.ProvinceId, Line1 = self.Line1, Line2 = self.Line2, Suburb = self.Suburb, City = self.City, WardNumber = self.WardNumber, Postcode = self.Postcode)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

