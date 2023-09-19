from flask import Blueprint

implied_vols = Blueprint('implied_vols', __name__)

from . import views