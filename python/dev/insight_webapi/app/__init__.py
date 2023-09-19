from flask import Flask
from .main.views import main_bp as main_blueprint
from .trading.views import trading_bp as trading_blueprint
from .positions.views import positions_bp as positions_blueprint
from .sod_positions.views import sod_positions_bp as sod_positions_blueprint
from .implied_vols.views import implied_vols_bp as implied_vols_blueprint
from .latest_rates.views import latest_rates_bp as latest_rates_blueprint
from .ppa.views import ppa_bp as ppa_blueprint
from .risk_greek.views import risk_greek_bp as risk_greek_blueprint
from flask_ldap3_login import LDAP3LoginManager

def create_app():
    app = Flask(__name__)
    app.register_blueprint(main_blueprint)
    app.register_blueprint(trading_blueprint)
    app.register_blueprint(positions_blueprint)
    app.register_blueprint(sod_positions_blueprint)
    app.register_blueprint(implied_vols_blueprint)
    app.register_blueprint(latest_rates_blueprint)
    app.register_blueprint(ppa_blueprint)
    app.register_blueprint(risk_greek_blueprint)

    ldap_manager = LDAP3LoginManager()
    ldap_manager.init_app(app)

    return app
