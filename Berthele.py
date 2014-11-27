# -*- coding: latin-1 -*-

"""Partnership with the Archives Municipales de Toulouse."""

__authors__ = 'User:Jean-Frédéric'

import os
import sys
from StringIO import StringIO
from uploadlibrary import metadata
import uploadlibrary.PostProcessing as commonprocessors
from uploadlibrary.UploadBot import DataIngestionBot, UploadBotArgumentParser, make_title
import processors

reload(sys)
sys.setdefaultencoding('utf-8')

front_titlefmt = ""
variable_titlefmt = "%(Titre)s"
rear_titlefmt = " - Fonds Berthelé - %(Cote)s"

class BertheleMetadataCollection(metadata.MetadataCollection):

    """Handling the metadata collection."""

    def handle_record(self, xml_element):
        """Handle a record."""

        image_metadata = self.get_metadata_from_xml_element(xml_element)
        cote = image_metadata['Cote']
        base_url = 'http://berthele.commonists.org/FRAC31555_49Fi%s.jpg'
        try:
            final_cote = "%04d" % int(cote[4:])
        except ValueError as e:
            cote_right = cote[4:]
            cote_parts = cote_right.split('/')
            try:
                final_cote = "%s_%04d" % (cote_parts[0], int(cote_parts[1]))
            except ValueError as e:
                raise metadata.UnreadableRecordException
        record = metadata.MetadataRecord(base_url % final_cote, image_metadata)
        title = make_title(image_metadata, front_titlefmt,
                           rear_titlefmt, variable_titlefmt)
        record.metadata['commons_title'] = title
        return record


def main(args):
    """Main method."""
    collection = BertheleMetadataCollection()
    xml_file = 'Test-Export_Berthele.xml'
    collection.retrieve_metadata_from_xml(xml_file, 'DocsFigures')
    alignment_template = 'User:Jean-Frédéric/AlignmentRow'.encode('utf-8')

    if args.prepare_alignment:
        for key, value in collection.count_metadata_values().items():
            collection.write_dict_as_wiki(value, key, 'wiki',
                                          alignment_template)

    if args.post_process:
        mapping_fields = ['geoname', 'persname', 'subject', 'corpname']
        mapper = commonprocessors.retrieve_metadata_alignments(mapping_fields,
                                                               alignment_template)
        mapping_methods = {
            'Format': (processors.parse_format, {}),
            'Analyse': (processors.look_for_date, {}),
            'geoname': (commonprocessors.process_with_alignment, {'mapper': mapper}),
            'persname': (commonprocessors.process_with_alignment, {'mapper': mapper}),
            'subject': (commonprocessors.process_with_alignment, {'mapper': mapper}),
            'corpname': (commonprocessors.process_with_alignment, {'mapper': mapper}),
            #'Technique': commonprocessors.map_and_apply_technique,
            }
        categories_counter, categories_count_per_file = collection.post_process_collection(mapping_methods)
        print metadata.categorisation_statistics(categories_counter, categories_count_per_file)

        reader = iter(collection.records)
        template_name = 'Commons:Batch_uploading/Fonds_Berthelé/Ingestion'.decode('utf-8').encode('utf-8')
        uploadBot = DataIngestionBot(reader=reader,
                                     front_titlefmt=front_titlefmt,
                                     rear_titlefmt=rear_titlefmt,
                                     variable_titlefmt=variable_titlefmt,
                                     pagefmt=template_name)

    if args.upload:
        uploadBot.doSingle()
    elif args.dry_run:
        #string = StringIO()
        #collection.write_metadata_to_xml(string)
        #print string.getvalue()
        uploadBot.dry_run()


if __name__ == "__main__":
    parser = UploadBotArgumentParser()
    arguments = parser.parse_args()
    main(arguments)
