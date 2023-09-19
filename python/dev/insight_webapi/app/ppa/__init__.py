from flask import Blueprint

PPA = Blueprint('ppa', __name__)

from . import views
