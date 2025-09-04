
#       _       _       
#      | |     | |      
#    __| | __ _| |_ ___ 
#   / _` |/ _` | __/ _ \
#  | (_| | (_| | ||  __/
#   \__,_|\__,_|\__\___|
# 
# Mixin to add to handle Dates                     
                    

from functools import singledispatch
from datetime import datetime, date
from typing import Union

@singledispatch
def _serialize(arg):
	"""
		Fallback if an unsupported type is passed in.
	"""
	raise TypeError(f"Cannot serialize type {type(arg).__name__!r}")

@_serialize.register
def _(arg: date) -> str:
	"""
		If the input is a date, return 'DD-MM-YYYY'.
	"""
	return arg.strftime("%d-%m-%Y")

@_serialize.register
def _(arg: str) -> date:
	"""
		If the input is a string 'DD-MM-YYYY', parse it back into a date.
	"""
	return datetime.strptime(arg, "%d-%m-%Y").date()

class DateTimeMixin:
	
	@staticmethod
	def today() -> date:
		"""
			Today's Date
		"""
		return date.today()

	@staticmethod
	def serialize(arg):
		"""
			Delegates to the module-level singledispatch function `_serialize`.
		"""
		return _serialize(arg)
	
	@staticmethod
	def is_past(value) -> bool :
		"""
			Compare the value with Today (date.today())
			If less than today then Return True.
			Signifies Past Date.
		"""
		if not isinstance(value, date):
			value = DateTimeMixin.serialize(value)
		
		if value < DateTimeMixin.today():
			return True
		
		return False
	
	@staticmethod
	def day_gap(value ) -> bool :
		"""
			Compare the value with Today (date.today())
			Get the gap between Date
			Signifies Past Date.
		"""
		if not isinstance(value, date):
			value = DateTimeMixin.serialize(value)
		
		return (DateTimeMixin.today() - value).days