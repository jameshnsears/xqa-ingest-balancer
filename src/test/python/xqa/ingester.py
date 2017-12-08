import argparse
import datetime
import hashlib
import logging
import mimetypes
import os
from uuid import uuid4

from lxml import etree
from lxml.etree import XMLSyntaxError
from proton import ConnectionException, Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

from xqa.commons import configuration


class Ingester(MessagingHandler):
    class IngestException(Exception):
        def __init__(self, message=None):
            if message:
                self.message = message

    ERROR_NO_XML_FILES_FOUND = 'no XML files found'
    ERROR_FILE_MIMETYPE = 'incorrect mimetype'
    ERROR_FILE_CONTENTS_NOT_WELL_FORMED = 'file not well-formed'

    def __init__(self, path_to_xml_candidate_files):
        MessagingHandler.__init__(self)
        logging.info(self.__class__.__name__)
        self._stopping = False
        self._path_to_xml_candidate_files = path_to_xml_candidate_files
        self._xml_files = self._find_xml_files()
        self._sent_index = 0

    def _find_xml_files(self):
        xml_files = []

        for root, _, filenames in os.walk(self._path_to_xml_candidate_files):
            for filename in filenames:
                path_to_filename = self._full_path_to_file(root, filename)
                try:
                    if self._can_file_be_used(path_to_filename, filename):
                        xml_files.append(path_to_filename)
                except Ingester.IngestException:
                    pass

        if not xml_files:
            logging.warning(Ingester.ERROR_NO_XML_FILES_FOUND)
            logging.info('EXIT')
            exit(-1)

        return sorted(xml_files)

    def _full_path_to_file(self, root, filename):
        return os.path.join(root, filename)

    def _can_file_be_used(self, path_to_filename, filename):
        if not self._check_file_mimetype_recognised(path_to_filename):
            logging.warning('N: %s: %s' % (Ingester.ERROR_FILE_MIMETYPE, filename))
            raise Ingester.IngestException(Ingester.ERROR_FILE_MIMETYPE)

        if not self._check_file_contents_well_formed(path_to_filename):
            logging.warning('N: %s: %s' % (Ingester.ERROR_FILE_CONTENTS_NOT_WELL_FORMED, filename))
            raise Ingester.IngestException(Ingester.ERROR_FILE_CONTENTS_NOT_WELL_FORMED)

        logging.debug('Y: %s' % path_to_filename)
        return True

    def _check_file_contents_well_formed(self, path_to_filename):
        try:
            etree.parse(path_to_filename)
            return True
        except XMLSyntaxError:
            return False

    def _check_file_mimetype_recognised(self, path_to_filename):
        if mimetypes.guess_type(path_to_filename) in [('application/xml', None), ('text/xml', None)]:
            return True
        return False

    def _contents_of_file(self, xml_file):
        with open(xml_file) as f:
            return f.read()

    def on_start(self, event):
        connection = event.container.connect(configuration.url_amqp)
        self.ingest_sender = event.container.create_sender(connection, configuration.queue_ingest)

    def on_accepted(self, event):
        if self._sent_index == len(self._xml_files):
            event.connection.close()

    def on_connection_closed(self, event):
        logging.info('EXIT')

    def on_disconnected(self, event):
        logging.warning('retry')
        self._sent_index -= 1

    def on_sendable(self, event):
        while event.sender.credit and self._sent_index < len(self._xml_files):
            logging.debug('credit=%d' % event.sender.credit)
            logging.info(self._xml_files[self._sent_index])

            message = Message(address=configuration.queue_ingest,
                              correlation_id=str(uuid4()),
                              creation_time=(datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds(),
                              durable=True,
                              body=self._contents_of_file(self._xml_files[self._sent_index]).encode('utf-8'))

            logging.info(
                '%s,%3s: creation_time=%s; correlation_id=%s; address=%s; reply_to=%s; expiry_time=%s; sha256(body)=%s',
                '>',
                self._sent_index,
                message.creation_time,
                message.correlation_id,
                message.address,
                message.reply_to,
                message.expiry_time,
                hashlib.sha256(message.body).hexdigest())

            event.sender.send(message)
            self._sent_index += 1

    def on_transport_error(self, event):
        logging.error('%s: %s' % (event.type, event.transport.condition.description))
        raise ConnectionException(event.transport.condition.description)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path', required=True,
                        help='path to folder containing xml files')
    args = parser.parse_args()

    try:
        Container(Ingester(args.path)).run()
    except (ConnectionException, KeyboardInterrupt):
        exit(-1)
