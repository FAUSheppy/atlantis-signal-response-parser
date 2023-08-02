import json
import argparse

from sqlalchemy import Column, Integer, String, Boolean, or_, and_
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func
import sqlalchemy
from sqlalchemy import Column, Integer, String, Boolean, or_, and_
from sqlalchemy.orm import declarative_base

base = declarative_base()
engine = None
sm = None

class SignalEnvelopeDB(base):
    
    __tablename__ = "envelopes"

    source           = Column(String)
    source_number    = Column(String)
    source_uuid      = Column(String, primary_key=True)
    source_device    = Column(String)
    source_timestamp = Column(String, primary_key=True)

    is_receipt = Column(Boolean)

    message = Column(String)
    state   = Column(String) # delived, read

class SignalEnvelope:

    def __init__(self, content):

        # containers #
        self.envelope = content.get("envelope")
        self.account  = content.get("account")

        # general #
        self.source        = self.envelope.get("source")
        self.source_number = self.envelope.get("source_number")
        self.source_uuid   = self.envelope.get("sourceUuid")
        self.source_name   = self.envelope.get("sourceName")
        self.source_device = self.envelope.get("sourceDevice")
        self.timestamp     = self.envelope.get("timestamp")

        # optional fields #
        self.message = None
        self.is_delivered = None
        self.is_read = None
        self.is_viewed = None

        # set special fields for data/normal message #
        self.data_message = self.envelope.get("dataMessage")
        if self.data_message:
            self.message = self.data_message.get("message")

        # set special fields for receipt message  #
        self.receipt_message = self.envelope.get("receiptMessage")
        if self.receipt_message:
            self.is_delivered = self.receipt_message.get("isDelivery")
            self.is_read = self.receipt_message.get("isRead")
            self.is_viewed = self.receipt_message.get("isViewed")

    def get_db_object(self):

        if self.is_delivered:
            status = "delivered"
        elif self. is_viewed:
            status = "viewed"
        else:
            status = None

        envelope_db = SignalEnvelopeDB(
                        source=self.source,
                        source_number=self.source_number,
                        source_uuid=self.source_uuid,
                        source_device=self.source_device,
                        message=self.message,
                        is_receipt=bool(self.receipt_message),
                        self.status=status)

        return envelope_db


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Signal Response Parser')
    parser.add_argument('--engine', help="e.g. postgresql+psycopg2://user:pass@localhost/ths")
    parser.add_argument("-f", "--file", required=True, help="File to read signal-json from")
    args = parser.parse_args()

    engine = sqlalchemy.create_engine(engine)
    sm = sessionmaker(bind=engine)
    base.metadata.create_all(engine)

    with open(args.file) as f:
        for l in f:
            content = json.loads(l)
            c = SignalEnvelope(content)
            print(json.dumps(content, indent=2))
