from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired,InputRequired,ValidationError

class ConnectServerForm(FlaskForm):
    serverIP = StringField('Neo4j Server IP',
                           validators=[DataRequired()])
    username = StringField('Username',
                        validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    connect = SubmitField('Connect')



class LoginForm(FlaskForm):
    username = StringField('Username',
                        [DataRequired()])
    
    def validate_username(form,field):
        print(field.data)
        if field.data!="gnadmin":
           raise ValidationError("Invalid username, Please check the username entered")
    
    password = PasswordField('Password', [DataRequired()])
    
    def validate_password(form,field):
        print(field.data)
        if field.data!="gnana":
           raise ValidationError("Incorrect password, Please check the password entered")
    
    login = SubmitField('Login')
