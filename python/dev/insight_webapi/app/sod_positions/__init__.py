from flask import Blueprint

sod_positions = Blueprint('sod_positions', __name__)

from . import views
