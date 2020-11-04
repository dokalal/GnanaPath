from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired


class ConnectServerForm(FlaskForm):
    serverIP = StringField('Neo4j Server IP',
                           validators=[DataRequired()])
    username = StringField('Username',
                        validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    connect = SubmitField('Connect')
