import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

if os.path.exists('.env.local'):
    load_dotenv('.env.local')
else:
    load_dotenv('.env')

class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'un mot de passe à garder secret'

    DB_USER = os.environ.get('DB_USER', 'utilisateur_par_defaut')
    DB_PASSWORD = os.environ.get('DB_PASSWORD', 'mot_de_passe_par_defaut')
    DB_HOST = os.environ.get('DB_HOST', 'localhost')
    DB_PORT = os.environ.get('DB_PORT', '3306')
    DB_NAME = os.environ.get('DB_NAME', 'database_par_defaut')

    SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    SQLALCHEMY_TRACK_MODIFICATIONS = False


def get_engine(echo=False):
    engine = create_engine(Config.SQLALCHEMY_DATABASE_URI, echo=echo)
    return engine


def get_session(echo=False):
    engine = get_engine(echo=echo)
    Session = sessionmaker(bind=engine)
    return Session
