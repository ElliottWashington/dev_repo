from flask import Flask
from flask_mail import Mail, Message

app = Flask(__name__)
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USE_SSL'] = True
app.config['MAIL_USERNAME'] = 'ewashington@scalptrade.com'
app.config['MAIL_PASSWORD'] = 'iLOVEscalp!' 

mail = Mail(app)

@app.route('/send_email')
def send_email():
	msg = Message('Test Email', sender='ewashington@scalptrade.com', recipients=['ewashington@scalptrade.com'])
	msg.body = 'This is a test email sent from Flask using the Flask-Mail extension.'
	mail.send(msg)
	return 'Email sent!'

if __name__ == '__main__':
	app.run()
