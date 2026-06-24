import os
from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required
from werkzeug.security import check_password_hash
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from app.utils.logger import setup_logger

auth_bp = Blueprint('auth', __name__)
login_manager = LoginManager()
limiter = Limiter(key_func=get_remote_address, default_limits=[])
logger = setup_logger("AUTH")


class AdminUser(UserMixin):
    id = "admin"


admin = AdminUser()


@login_manager.user_loader
def load_user(user_id):
    if user_id == "admin":
        return admin
    return None


@login_manager.unauthorized_handler
def unauthorized():
    return redirect(url_for('auth.login', next=request.path))


@auth_bp.route('/login', methods=['GET', 'POST'])
@limiter.limit("10 per minute")
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')

        expected_username = os.environ.get('ADMIN_USERNAME', 'admin')
        password_hash = os.environ.get('ADMIN_PASSWORD_HASH', '')

        if username == expected_username and password_hash and check_password_hash(password_hash, password):
            login_user(admin, remember=False)
            logger.info("login | SUCCESS | username=%s ip=%s", username, request.remote_addr)
            next_page = request.args.get('next') or url_for('dashboard.dashboard_page')
            return redirect(next_page)

        logger.warning("login | FAILED | username=%s ip=%s", username, request.remote_addr)
        flash('نام کاربری یا رمز عبور اشتباه است.')

    return render_template('login.html')


@auth_bp.route('/logout')
@login_required
def logout():
    logger.info("logout | ip=%s", request.remote_addr)
    logout_user()
    return redirect(url_for('auth.login'))
