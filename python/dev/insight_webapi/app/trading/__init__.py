from flask import Blueprint

trading = Blueprint('trading', __name__)

from . import views
