from flask import Blueprint

latest_rates = Blueprint('latest_rates', __name__)

from . import views