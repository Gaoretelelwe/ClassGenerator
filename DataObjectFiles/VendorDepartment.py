from sqlalchemy import Table, Column, Integer, DateTime, String, Float, MetaData

from DataAccess.DataAccess import DataAccess
from Util.ErrorHandler import InsertError, UpdateError, DeleteError, FetchError

class VendorDepartment():

	def __init__(self, VendorDepartmentId = None, VendorId = None, DepartmentTypeId = None, ContactPersonId = None, data_access = None):
		self.meta = MetaData()
		self.data_access = data_access

		self.IsFetched = False
		self.IsInserted = False
		self.IsUpdated = False
		self.IsDeleted = False

		self.content = Table(
			'VendorDepartment', self.meta,
			Column('VendorDepartmentId', Integer, primary_key = True),
			Column('VendorId', Integer),
			Column('DepartmentTypeId', Integer),
			Column('ContactPersonId', Integer),
		)

		self.VendorDepartmentId = VendorDepartmentId
		self.VendorId = VendorId
		self.DepartmentTypeId = DepartmentTypeId
		self.ContactPersonId = ContactPersonId

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

	def DBFetch(self, VendorDepartmentId):
		try:
			if not self.IsInserted and not self.IsFetched:
				self._db_fetch_check()
				self._db_fetch(VendorDepartmentId)
		except FetchError as fetch_error:
			raise fetch_error

	def Delete(self):
		try:
			if self.IsInserted or self.IsFetched:
				self._db_delete_check()
				self._db_delete()
			else:
				raise DeleteError('The VendorDepartment is neither fetched nor inserted.')
		except DeleteError as delete_error:
			raise delete_error

	def _db_insert_check(self):
		if self.IsDeleted:
			raise InsertError('The VendorDepartment is deleted.')

		if self.IsInserted:
			raise InsertError('The VendorDepartment is deleted.')

		if self.IsUpdated:
			raise InsertError('The VendorDepartment is deleted.')

		if self.VendorId == None:
			raise InsertError('Please make sure that VendorId has a value.')

		if self.DepartmentTypeId == None:
			raise InsertError('Please make sure that DepartmentTypeId has a value.')

		if self.ContactPersonId == None:
			raise InsertError('Please make sure that ContactPersonId has a value.')

	def _db_insert(self):
		if not self.IsInserted and not self.IsFetched and not self.IsUpdated:
			result = self.data_access.connection.execute(self.content.insert(), [
				{'VendorId':self.VendorId, 'DepartmentTypeId':self.DepartmentTypeId, 'ContactPersonId':self.ContactPersonId},
			])
			self.VendorDepartmentId = result.inserted_primary_key
			self.IsInserted = True
			self.IsFetched = True

	def _db_fetch_check(self):
		if self.IsDeleted:
			raise FetchError('The VendorDepartment is deleted.')

	def _db_fetch(self):
		if not self.IsFetched:
			s = self.content.select().where(self.content.c.VendorDepartmentId == self.VendorDepartmentId)
			result = self.data_access.connection.execute(s)
			row = result.first()

			if row == None:
				raise FetchError('The VendorDepartment does not exist. VendorDepartment Id is {0}.'.format(str(VendorDepartmentId)))
			else:
				#Get results and assign them to class variables

				self.VendorDepartmentId = row[0]
				self.VendorId = row[1]
				self.DepartmentTypeId = row[2]
				self.ContactPersonId = row[3]
				self.IsFetched = True

	def _db_delete_check(self):
		if self.IsDeleted:
			raise DeleteError('The VendorDepartment is deleted.')

	def _db_delete(self):
		s = self.content.delete().where(self.content.c.VendorDepartmentId == self.VendorDepartmentId)
		self.data_access.connection.execute(s)

		self.IsDeleted = True

	def _db_update_check(self):
		if self.IsDeleted:
			raise UpdateError('The VendorDepartment is deleted.')

	def _db_update(self):
		s = self.content.update().where(self.content.c.VendorDepartmentId == self.VendorDepartmentId).values(VendorId = self.VendorId, DepartmentTypeId = self.DepartmentTypeId, ContactPersonId = self.ContactPersonId)
		self.data_access.connection.execute(s)

		self.IsUpdated = True

