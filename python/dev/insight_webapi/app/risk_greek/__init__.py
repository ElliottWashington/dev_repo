from flask import Blueprint

trading = Blueprint('risk_greek', __name__)

from . import views
