import logging
import urllib
import tempfile
from datetime import datetime, timedelta, timezone
import dateparser
from arcgis.gis import GIS
from arcgis.features import FeatureLayer, Feature
from arcgis.geometry import Point, Polyline
from .dasclient import DasClient
from .version import __version__
from .schemas import EREvent, ERLocation

class AgolTools(object):

    UPDATE_TIME_PADDING = 24*60 # minutes

    event_types = None
    ER_TO_ESRI_FIELD_TYPE = {
        'string': 'esriFieldTypeString',
        'number': 'esriFieldTypeDouble',
        'default': 'esriFieldTypeString'
    }

    {'name':'REPORT_TIME', 'alias': 'Report Time', 'type': 'esriFieldTypeDate'},
    {'name':'REPORTED_BY', 'alias': 'Reported By', 'type': 'esriFieldTypeString'},
    {'name':'LATITUDE', 'alias': 'Latitude', 'type': 'esriFieldTypeDouble'},
    {'name':'LONGITUDE', 'alias': 'Longitude', 'type': 'esriFieldTypeDouble'},

    def __init__(self, er_token, er_service_root, esri_url, esri_username, esri_password):
        """
        Initialized an AgolTools object.  Establishes sessions to both ER
        and ArcGIS Online

        :param er_token: Token for connecting to EarthRanger
        :param er_service_root: URL to the API service root of EarthRanger
        :param esri_url: URL for logging in to ArcGIS Online
        :param esri_username: Username for ArcGIS Online
        :param esri_password: Password for ArcGIS Online
        :return: None
        """

        self.logger = logging.getLogger(self.__class__.__name__)

        self.das_client = DasClient(token=er_token, service_root=er_service_root)
        self.logger.info(f"Logged in to ER: {er_service_root}")

        self.gis = GIS(esri_url, esri_username, esri_password)
        self.logger.info("Logged in to AGOL as " + str(self.gis.properties.user.username))

        return

    def _clean_field_alias(self, alias):
        """
        Exists as an interface to allow calling functions to modify the handling
        of a field alias.

        :param field: Field alias to clean
        :return: Clean field alias
        """
        return alias

    def _clean_field_value(self, value):
        """
        Exists as an interface to allow callers to modify the handling of a
        field's value.

        :param value: Field value to clean
        :return: Clean field value
        """
        return value


    def _clean_field_name(self, field):
        """
        Cleans a field name to make sure that Esri doesn't choke on it.
        Currently, it just replaces -'s with _'s, but there could be other
        characters discovered in the future.

        :param field: Field name to clean
        :return: Clean field name
        """
        field = field.replace("-","_")
        return field

    def _field_already_exists(self, field, esri_layer, additional_fields):
        """
        Helper method to checks if a field already exists in either Esri or in
        the list of fields we're about to submit

        :param field: Field to check
        :param esri_layer: AGO layer to look for that field name
        :param additional_fields: Additional list to check for the field.  Each entry is of the format [name, type].
        :return: Whether or not the field name is already in either AGO or the passed-in list
        """
        for esri_field in esri_layer.properties.fields:
            if(esri_field.name == field):
                return True
        for new_field in additional_fields:
            if(new_field[0] == field):
                return True
        return False

    def _ensure_attributes_in_layer(self, esri_layer, fields):
        """
        Makes sure that a list of attributes is already in an AGO layer.  If one
        is missing, it's created.

        :param esri_layer: AGO layer to check
        :param fields: Fields to check or add to the layer
        :return: None
        """
        new_fields = []
        for field in fields:
            if(not(self._field_already_exists(field['name'], esri_layer, []))):
                new_fields.append(field)

        if(not new_fields):
            self.logger.info("Fields already exist in layer")
        else:
            self.logger.info("Creating fields in layer")
            esri_layer.manager.add_to_definition({'fields': new_fields})

    def _get_existing_esri_points(self, points_layer, oldest_date, er_subject_id = None):
        """
        Grabs existing Esri track points from AGOL.

        :param points_layer: AGO layer to check
        :param oldest_date: Start date for date range
        :param er_subject_id: ER subject ID for which to grab points (optional)
        :return: None
        """

        query = "EditDate > '" + oldest_date.strftime("%Y-%m-%d %H:%M:%S") + "'"

        if(er_subject_id != None):
            query += f" and ER_SUBJECT_ID = '{er_subject_id}'"

        existing = points_layer.query(where=query)
        existing_ids = {}
        for event in existing:
            existing_ids[str(event.attributes['ER_OBSERVATION_ID'])] = [event.attributes['OBJECTID'], event.attributes['EditDate']]
        return existing_ids


    def _get_existing_esri_events(self, events_layer, oldest_date):
        """
        Loads the existing EarthRanger reports contained within an AGO layer.
        EarthRanger reports are determined by the field ER_REPORT_NUMBER not
        being blank.

        :param esri_layer: AGO layer to query
        :return: Dictionary with keys = ER serial number and value = AGO object ID
        """
        query = "ER_REPORT_NUMBER<>''"
        if(oldest_date):
            query += " and EditDate > '" + oldest_date.strftime("%Y-%m-%d %H:%M:%S") + "'"

        try:
            existing = events_layer.query(where=query)
        except Exception as e:
            if("'Invalid field: ER_REPORT_NUMBER' parameter is invalid" in str(e)):
                return {}
            raise e

        existing_ids = {}
        for event in existing:
            existing_ids[str(event.attributes['ER_REPORT_NUMBER'])] = [event.attributes['OBJECTID'], event.attributes['EditDate']]
        return existing_ids

    def _get_existing_esri_tracks(self, esri_layer):
        """
        Loads the existing EarthRanger tracks contained within an AGO layer.
        EarthRanger tracks are determined by the field ER_ID not being blank.

        :param esri_layer: AGO layer to query
        :return: Dictionary with keys = ER subject ID and value = AGO object ID
        """
        existing = esri_layer.query(where="'ER_ID'<>''")
        existing_ids = {}
        for event in existing:
            existing_ids[event.attributes['ER_ID']] = event.attributes['OBJECTID']
        return existing_ids

    def __get_value_map_from_prop_def(self, schema_defs, key):
        """
        Helper method to find a schema definition map to use for mapping field
        values to strings

        :param schema_defs: The schema definition for the field
        :param key: The field to look for
        :return: A map of key/value pairs for input/output field values, if one
            exists within the schema definition.  An empty dictionary is
            returned otherwise.
        """
        value_map = {}
        for schema_def in schema_defs:
            for item in schema_def.get('items', []):
                if(('key' in item) and (item['key'] == key)):
                        if('titleMap' in item):
                            for map_item in item['titleMap']:
                                value_map[map_item['value']] = map_item['name']
                            return value_map
        return {}

    def _get_er_field_definitions(self, event_type):
        """
        Loads information about ER schemas for an event type.

        :param event_type: Event type for which to load information
        :return: Tuple containing the name of the event type and a map of the
            event type's fields.  For each event type, the ER schema for that
            event is copied, and an attribute "value_map" is added,
            which helps map the internal value of variables to the user-friendly
            string versions (ex. "lions_observed" -> "Lions observed").  This
            takes into account both enum maps within the schema properties as
            well as key maps within the schema definition.  The structure of the
            returned data structure is as follows:
            {
                    er_property_name:
                    {
                        (Copy of the properties from the ER event schema),
                        'value_map':
                        {
                            input_value: destination_value,
                            ...
                        }
                    },...
            }
        """
        field_defs = {}

        schema = self.das_client.get_event_schema(event_type)
        props = schema['schema']['properties']
        for prop_name in props:
            prop = props[prop_name]
            field_defs[prop_name] =  prop
            if('key' in prop):
                field_defs[prop_name]['value_map'] = self.__get_value_map_from_prop_def(schema['definition'], prop['key'])
            elif('enumNames' in field_defs[prop_name]):
                field_defs[prop_name]['value_map'] = field_defs[prop_name].pop('enumNames')

        return (schema['schema']['title'], field_defs)

    def _add_fields_to_layer(self, fields, esri_layer):
        """
        Adds a list of fields to an AGO layer.  Right now we create all fields
        as esriFieldTypeString fields.  This is something to improve in the
        future.

        :param fields: List of field names descriptors.  Each is a 3-element array of field name, Esri field type and (optionally) field alias
        :return: None
        """

        new_fields = []
        for field in fields:
            new_field = {'name': self._clean_field_name(field[0]), 'type': field[1]}

            if(len(field) == 3):
                new_field['alias'] = self._clean_field_alias(field[2])

            new_fields.append(new_field)
        result = esri_layer.manager.add_to_definition({'fields': new_fields})
        if(result['success'] != True):
            raise Exception(f"Error when creating fields: {result}")
        return

    def _chunk(self, lst, chunk_size):
        """
        Simple helper method to chunk a list into pieces

        :param lst: List to chunk
        :param chunk_size: How large of chunks to return
        :return: Iterator over the chunks
        """
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]

    def _upsert_features(self, add_features, update_features, esri_layer, chunk_size=5):
        """
        Adds or updates a list of features in an AGO layer

        :param add_features: List of features to add (as either Esri Feature objects or similar dictionaries)
        :return: None
        """
        added = self._add_features(add_features, esri_layer, chunk_size)
        updated = self._update_features(update_features, esri_layer, chunk_size)
        return (added, updated)

    def _add_features(self, add_features, esri_layer, chunk_size=5):
        """
        Adds features to an Esri layer

        :param add_features: A list of features to create
        :param esri_layer: The feature layer to add to
        :param chunk_size: How many features to add at a time (particularly
            import when updating large line features)
        :return: Number of added features
        """
        added = 0
        for chunk in self._chunk(add_features, chunk_size):
            results = esri_layer.edit_features(adds = chunk)
            for result in results['addResults']:
                if(result['success'] != True):
                    self.logger.error(f"Error when creating feature: {result}")
                else:
                    added += 1

        return added

    def _update_features(self, update_features, esri_layer, chunk_size=5):
        """
        Updates features in an Esri layer

        :param update_features: A list of features to update, each of which
            contains an Esri global ID to identify which feature to update
        :param esri_layer: The feature layer to update within
        :param chunk_size: How many features to update at a time (particularly
            import when updating large line features)
        :return: Number of updated features
        """
        updated = 0
        for chunk in self._chunk(update_features, chunk_size):
            results = esri_layer.edit_features(updates = chunk)
            for result in results['updateResults']:
                if(result['success'] != True):
                    self.logger.error(f"Error when updating feature: {result}")
                else:
                    updated += 1

        return updated

    def _replace_attachments(self, esri_layer, oldest_date, event_files):
        """
        Replaces the attachments of Esri features with the attachments described
        by the event_files parameter.

        :param esri_layer: The AGO layer to work with.  Features are loaded
            which match the ER report numbers specified within the event_files
            paramter
        :param oldest_date: As far back to look for matching Esri features
        :param event_files: Describes the files to upload.  This is a dictionary of the form:
            {
                er_report_number: {
                    'url': URL to load the file from
                    'filename': Filename to give the file
                }
            }
        :return: None
        """
        existing_events = self._get_existing_esri_events(esri_layer, oldest_date)
        tmpdir = tempfile.TemporaryDirectory()

        for event in event_files.keys():
            esri_object_id = existing_events[event][0]
            existing_attachments = esri_layer.attachments.get_list(esri_object_id)

            for existing_file in existing_attachments:
                self.logger.info(f"Removing attachment {existing_file['name']} from feature {esri_object_id}")
                esri_layer.attachments.delete(esri_object_id, existing_file['id'])

            for file in event_files[event]:
                self.logger.info(f"Adding attachment {file['filename']} from ER event {event} to Esri feature {esri_object_id}")
                tmppath = tmpdir.name + "/" + file['filename']
                result = self.das_client.get_file(file['url'])
                open(tmppath, 'wb').write(result.content)
                esri_layer.attachments.add(esri_object_id, tmppath)

        tmpdir.cleanup()

    def upsert_tracks_from_er(self, esri_layer, since):
        """
        Queries all EarthRanger subjects from the active ER connection, grabs
        their tracks, and creates or updates polylines in AGO to match.  The
        EarthRanger subject ID is stored in AGO as a parameter ER_ID on each
        linestring.

        Right now, this method is very brute force: It either adds a track or
        replaces the whole track.  There are also not (yet) any parameters
        around track length, date ranges, which subjects to include, etc.

        :param esri_layer: The AGO layer to upsert
        :param since: The start date/time of the track to load
        :return: None
        """
        self._ensure_attributes_in_layer(esri_layer, [
            {'name':'ER_ID', 'alias': 'ER ID', 'type': 'esriFieldTypeString'},
            {'name':'SUBJECT_NAME', 'alias': 'Subject Name', 'type': 'esriFieldTypeString'}
        ])

        subjects = self.das_client.get_subjects()
        existing_tracks = self._get_existing_esri_tracks(esri_layer)
        features_to_add = []
        features_to_update = []

        if(since == None):
            since = datetime.now(tz=timezone.utc) - timedelta(days=30)

        for subject in subjects:

            if(('last_position_date' not in subject) or (subject['last_position_date'] == None)):
                continue

            if(subject['id'] in existing_tracks.keys()):
                last_position_date = dateparser.parse(subject['last_position_date'])
                cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=self.UPDATE_TIME_PADDING)

                if(last_position_date < cutoff):
                    self.logger.info(f"Subject {subject['name']} not recently updated... Skipping.")
                    continue

            results = self.das_client.get_subject_tracks(subject_id = subject['id'], start=since)
            self.logger.debug(f"Loaded {len(results['features'])} tracks from ER for subject {subject['name']}")

            for feature in results['features']:

                if(not('geometry' in feature.keys()) or (feature['geometry'] == None)
                    or not('coordinates' in feature['geometry'].keys()) or (feature['geometry']['coordinates'] == None)):
                    continue

                self.logger.debug(f"Track for {subject['name']} contains {len(feature['geometry']['coordinates'])} points")

                polyline = {
                    "geometry": {
                        "paths": [feature['geometry']['coordinates']],
                        "spatialReference": {"wkid" : 4326}
                    },
                    "attributes": {
                        "ER_ID": subject['id'],
                        "SUBJECT_NAME": subject['name']
                    }
                }

                if(str(subject['id']) in existing_tracks.keys()):
                    polyline['attributes']['OBJECTID'] = existing_tracks[str(subject['id'])]
                    features_to_update.append(polyline)
                else:
                    features_to_add.append(polyline)

        if((len(features_to_add) > 0) or (len(features_to_update) > 0)):
            (added, updated) = self._upsert_features(features_to_add, features_to_update, esri_layer, 2)
            self.logger.info(f"Created {added} and updated {updated} track features in Esri")
        else:
            self.logger.info(f"No tracks to add or update")

    def upsert_events_from_er(self, esri_layer, oldest_date = None, include_attachments = True,
        include_incidents = True):
        """
        Queries all EarthRanger events from the active ER connection and creates
        or updates points in AGO to match.  The EarthRanger report serial number
        is stored in AGO as the parameter ER_REPORT_NUMBER, which is used as the
        unique identifer.

        Right now, this method is very brute force: It either adds or replaces
        every event.  There are not (yet) any parameters around which events to
        include.  It also does not include attachments, notes, or take incidents
        into consideration.

        :param esri_layer: The AGO layer to upsert
        :param oldest_date: The start-date of the date range to synchronize
        :param include_attachments: Whether to synchronize event attachment files
        :param include_incidents: Whether to include ER incidents with a reference to its included reports
        :return: None
        """

        base_attributes = [
            {'name':'ER_REPORT_NUMBER', 'alias': 'ER Report Number', 'type': 'esriFieldTypeInteger'},
            {'name':'ER_REPORT_TIME', 'alias': 'ER Report Time', 'type': 'esriFieldTypeDate'},
            {'name':'ER_REPORT_TITLE', 'alias': 'ER Report Title', 'type': 'esriFieldTypeString'},
            {'name':'ER_REPORT_TYPE', 'alias': 'ER Report Type', 'type': 'esriFieldTypeString'},
            {'name':'REPORTED_BY', 'alias': 'Reported By', 'type': 'esriFieldTypeString'},
            {'name':'LATITUDE', 'alias': 'Latitude', 'type': 'esriFieldTypeDouble'},
            {'name':'LONGITUDE', 'alias': 'Longitude', 'type': 'esriFieldTypeDouble'}]

        if(include_incidents):
            base_attributes.append({'name':'PARENT_INCIDENT',
                'alias': 'Parent Incident', 'type': 'esriFieldTypeString'})

        self._ensure_attributes_in_layer(esri_layer, base_attributes)

        if(oldest_date == None):
            oldest_date = datetime.now(tz=timezone.utc) - timedelta(days=30)

        er_events = self.das_client.get_events(include_notes = True,
            include_related_events = include_incidents, include_files = True,
            include_updates = False, oldest_update_date = oldest_date)

        existing_events = self._get_existing_esri_events(esri_layer, oldest_date)
        self.logger.info(f"Loaded {len(existing_events)} existing events from Esri")

        features_to_add = []
        features_to_update = []
        fields_to_add = []
        er_field_types = {}
        er_event_type_names = {}
        er_event_files = {}
        event_count = 0
        for event in er_events:
            event_count += 1
            if(str(event['serial_number']) in existing_events.keys()):
                esri_event = existing_events[str(event['serial_number'])]
                esri_update_time = esri_event[1]
                er_update_time = dateparser.parse(event['updated_at']).timestamp() * 1000

                # If the Esri event was updated more recently than an hour after the ER one was, skip it
                if(esri_update_time > (er_update_time + self.UPDATE_TIME_PADDING * 60*1000)):
                    continue

            if(not include_incidents and event.get('is_collection')):
                continue

            feature = {
                "attributes":{
                    'ER_REPORT_NUMBER': str(event['serial_number']),
                    'ER_REPORT_TIME': dateparser.parse(event['time']).timestamp()*1000
                }
            }

            if(event.get('location')):
                feature['geometry'] = Point(
                    {'y': event['location']['latitude'], 'x': event['location']['longitude'],
                    'spatialReference': {'wkid': 4326}})

                feature['attributes']['LATITUDE'] = str(event['location']['latitude'])
                feature['attributes']['LONGITUDE'] = str(event['location']['longitude'])

            if(event.get('reported_by')):
                feature['attributes']['REPORTED_BY'] = str(event['reported_by'].get('name', ''))

            if(event['event_type'] not in er_field_types):
                er_event_type_names[event['event_type']], er_field_types[event['event_type']] = self._get_er_field_definitions(event['event_type'])

            feature['attributes']['ER_REPORT_TYPE'] = self._clean_field_value(er_event_type_names[event['event_type']])

            if(event['title'] == None):
                feature['attributes']['ER_REPORT_TITLE'] = self._clean_field_value(feature['attributes']['ER_REPORT_TYPE'])
            else:
                feature['attributes']['ER_REPORT_TITLE'] = self._clean_field_value(str(event['title']))

            for field in event['event_details'].keys():

                if(field not in er_field_types[event['event_type']].keys()):
                    self.logger.warning(f"Additional data entry field {field} for event {event['serial_number']} not in event type model - skipping")
                    continue

                field_def = er_field_types[event['event_type']][field]
                field_type = field_def.get('type', 'string')
                esri_type = self.ER_TO_ESRI_FIELD_TYPE.get(field_type, self.ER_TO_ESRI_FIELD_TYPE['default'])
                field_name = self._clean_field_name(field + "_" + field_type)

                if not(self._field_already_exists(field_name, esri_layer, fields_to_add)):
                    fields_to_add.append([field_name, esri_type, field_def.get('title', field)])

                field_value = event['event_details'][field]
                if(type(field_value) != list):
                    field_value = [field_value]

                if('value_map' in field_def.keys()):
                    for i in range(0, len(field_value)):
                        field_value[i] = field_def['value_map'].get(field_value[i], field_value[i])
                for i in range(0, len(field_value)):
                    field_value[i] = self._clean_field_value(str(field_value[i]))
                feature['attributes'][field_name] = ",".join(field_value)

            if(include_incidents):
                for potential_parent in event.get('is_contained_in'):
                    if(potential_parent.get('type') == 'contains'):
                        feature['attributes']['PARENT_INCIDENT'] = potential_parent['related_event']['serial_number']
                        break


            if(str(event['serial_number']) in existing_events.keys()):
                feature['attributes']['OBJECTID'] = existing_events[str(event['serial_number'])][0]
                features_to_update.append(feature)
            else:
                features_to_add.append(feature)

            if(event['files']):
                er_event_files[str(event['serial_number'])] = []
                for file in event['files']:
                    er_event_files[str(event['serial_number'])].append({
                        'url': file['url'],
                        'filename': file['filename']
                    })

        self.logger.info(f"Processed {event_count} events from ER")
        if(not fields_to_add):
            self.logger.info(f"No new fields to add to the Esri layer.")
        else:
            new_fields = self._add_fields_to_layer(fields_to_add, esri_layer)
            self.logger.info(f"Added {len(fields_to_add)} new fields to the Esri layer")

        if((len(features_to_add) > 0) or (len(features_to_update) > 0)):
            (added, updated) = self._upsert_features(features_to_add, features_to_update, esri_layer, 50)
            self.logger.info(f"Created {added} and updated {updated} point features in Esri")
        else:
            self.logger.info(f"No event features to add or update")

        if(include_attachments):
            if(len(er_event_files) > 0):
                self._replace_attachments(esri_layer, oldest_date, er_event_files)
            else:
                self.logger.info(f"No attachments to add")

    def upsert_track_points_from_er(self, esri_layer, oldest_date = None):
        """
        Updates an AGOL point layer, adding any missing observation points from
        EarthRanger.  Each point in the track layer represents a single
        observation object from EarthRanger.  Points that already exist in AGOL
        are skipped.

        :param esri_layer: The AGO layer to upsert
        :param oldest_date: The start-date of the date range to synchronize
        :return: None
        """

        base_attributes = [
            {'name':'ER_OBSERVATION_ID', 'alias': 'Observation ID', 'type': 'esriFieldTypeString'},
            {'name':'SUBJECT_NAME', 'alias': 'Subject Name', 'type': 'esriFieldTypeString'},
            {'name':'OBSERVATION_TIME', 'alias': 'Observation Time', 'type': 'esriFieldTypeDate'},
            {'name':'ER_SUBJECT_ID', 'alias': 'Subject ID', 'type': 'esriFieldTypeString'},
            {'name':'LATITUDE', 'alias': 'Latitude', 'type': 'esriFieldTypeDouble'},
            {'name':'LONGITUDE', 'alias': 'Longitude', 'type': 'esriFieldTypeDouble'}]
        self._ensure_attributes_in_layer(esri_layer, base_attributes)

        if(oldest_date == None):
            oldest_date = datetime.now(tz=timezone.utc) - timedelta(days=30)

        subjects = self.das_client.get_subjects()

        for subject in subjects:
            if(not subject.get('tracks_available')):
                continue

            last_position_date = subject.get('last_position_date')
            if(not last_position_date):
                continue

            last_position_datetime = dateparser.parse(last_position_date)
            if(last_position_datetime < oldest_date):
                continue

            er_observations = self.das_client.get_subject_observations(
                subject['id'], oldest_date, None, 0, False, 10000)

            existing_points = self._get_existing_esri_points(esri_layer, oldest_date, subject['id'])
            self.logger.info(f"Loaded {len(existing_points)} existing points from Esri for subject {subject['name']}")

            features_to_add = []
            point_count = 0
            for point in er_observations:
                point_count += 1
                if(str(point['id']) in existing_points.keys()):
                    continue

                feature = {
                    "attributes": {
                        'ER_OBSERVATION_ID': point['id'],
                        'ER_SUBJECT_ID': subject['id'],
                        'SUBJECT_NAME': subject['name'],
                        'OBSERVATION_TIME': dateparser.parse(point['recorded_at']).timestamp()*1000
                    }
                }

                if(point.get('location')):
                    feature['geometry'] = Point(
                        {'y': point['location']['latitude'], 'x': point['location']['longitude'],
                        'spatialReference': {'wkid': 4326}})

                    feature['attributes']['LATITUDE'] = str(point['location']['latitude'])
                    feature['attributes']['LONGITUDE'] = str(point['location']['longitude'])

                features_to_add.append(feature)

            self.logger.info(f"Processed {point_count} track points from ER")
            if(len(features_to_add) > 0):
                (added, updated) = self._upsert_features(features_to_add, [], esri_layer, 50)
                self.logger.info(f"Created {added} point features in Esri")

if __name__ == '__main__':
    pass
