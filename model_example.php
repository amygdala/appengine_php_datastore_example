<?php

// ....

require_once 'config.php';
require_once 'FeedModel.php';

DatastoreService::setInstance(new DatastoreService($google_api_config));

$feed_url = 'http://www.sciam.com/xml/sciam.xml';
$feed_model = new FeedModel($feed_url);

// save the instance to the datastore
$feed_model->put();

// now, try fetching the saved model from the datastore

$kname = sha1($feed_url);
// fetch the feed with that key, as part of the transaction
$feed_model_fetched = FeedModel::fetch_by_name($kname)[0];

echo "fetched feed model with subscriber url: " . $feed_model_fetched->getSubscriberUrl();
