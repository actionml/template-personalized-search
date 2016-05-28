"""
Import sample data for recommendation engine
"""

import predictionio
import argparse
import random
import datetime
import pytz

FIELDS_DELIMITER = "\t"
PROPERTIES_DELIMITER = ","
PRIMARY_EVENT = "add-to-basket"
SEARCH_EVENT = "search"
SEED = 1


def import_events(client, file):
    f = open(file, 'r')
    random.seed(SEED)
    count = 0
    # year, month, day[, hour[, minute[, second[
    #event_date = datetime.datetime(2015, 8, 13, 12, 24, 41)
    now_date = datetime.datetime.now(pytz.utc) # - datetime.timedelta(days=2.7)
    current_date = now_date
    event_time_increment = datetime.timedelta(days= -0.8)
    available_date_increment = datetime.timedelta(days= 0.8)
    event_date = now_date - datetime.timedelta(days= 2.4)
    available_date = event_date + datetime.timedelta(days=-2)
    expire_date = event_date + datetime.timedelta(days=2)
    print "Importing data..."

    for line in f:
        # get 3 fields
        data = line.translate(None, '"[]\r\n').split(FIELDS_DELIMITER)
        user_id = data[0]
        search_phrases = data[1].rsplit(PROPERTIES_DELIMITER)
        product_id = data[2]

        if (product_id != "NULL"):
            client.create_event(
                event=PRIMARY_EVENT,
                entity_type="user",
                entity_id=user_id,
                target_entity_type="item",
                target_entity_id=product_id,
                event_time = current_date
            )
            print "Event: " + PRIMARY_EVENT + " entity_id: " + user_id + " target_entity_id: " + product_id + \
                  " current_date: " + current_date.isoformat()
            current_date += event_time_increment

        for phrase in search_phrases:  # assumes other event type is 'view'
            client.create_event(
                event=SEARCH_EVENT,
                entity_type="user",
                entity_id=user_id,
                target_entity_type="item",  # type of item in this action
                target_entity_id=phrase,
                event_time = current_date
            )
            print "Event: " + SEARCH_EVENT + " entity_id: " + user_id + " target_entity_id: " + phrase + \
                  " current_date: " + current_date.isoformat()
            current_date += event_time_increment
        count += 1
    f.close()
    print "%s lines are imported." % count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description="Import sample data for recommendation engine")
    parser.add_argument('--access_key', default='invald_access_key')
    parser.add_argument('--url', default="http://localhost:7070")
    parser.add_argument('--file', default="./data/sample-handmade-data.txt")

    args = parser.parse_args()
    print args

    client = predictionio.EventClient(
        access_key=args.access_key,
        url=args.url,
        threads=5,
        qsize=500
    )
    import_events(client, args.file)
