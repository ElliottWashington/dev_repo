from flask import Blueprint

positions = Blueprint('positions', __name__)

from . import views
