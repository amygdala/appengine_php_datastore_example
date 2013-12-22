<?php

// Example code for connecting to the Google Cloud Datastore

require_once 'config.php';
require_once 'Google/Client.php';
require_once 'Google/Auth/AssertionCredentials.php';
require_once 'Google/Service/Datastore.php';

// assumes that an array like the following is defined in config.php
// $google_api_config = [
//   'application-id' => 'A name for your app engine app',
//   'service-account-name' => 'xxx@developer.gserviceaccount.com',
//   'private-key' => file_get_contents('xxx-privatekey.p12'),
//   'dataset-id' => 'your-app-id'
// ];

function create_entity() {
  $entity = new Google_Service_Datastore_Entity();
  $entity->setKey(createKeyForTestItem());
  $string_prop = new Google_Service_Datastore_Property();
  $string_prop->setStringValue("test field string value");
  $property_map = [];
  $property_map["testfield"] = $string_prop;
  $entity->setProperties($property_map);
  return $entity;
}

function createKeyForTestItem() {
  $path = new Google_Service_Datastore_KeyPathElement();
  $path->setKind("testkind");
  $path->setName("testkeyname");
  $key = new Google_Service_Datastore_Key();
  $key->setPath([$path]);
  return $key;
}

function create_test_request() {
  $entity = create_entity();

  $mutation = new Google_Service_Datastore_Mutation();
  $mutation->setUpsert([$entity]);

  $req = new Google_Service_Datastore_CommitRequest();
  $req->setMode('NON_TRANSACTIONAL');
  $req->setMutation($mutation);
  return $req;
}

$scopes = [
    "https://www.googleapis.com/auth/datastore",
    "https://www.googleapis.com/auth/userinfo.email",
  ];

$client = new Google_Client();
$client->setApplicationName($google_api_config['application-id']);
$client->setAssertionCredentials(new Google_Auth_AssertionCredentials(
  $google_api_config['service-account-name'],
  $scopes, $google_api_config['private-key']));

$service = new Google_Service_Datastore($client);
$service_dataset = $service->datasets;
$dataset_id = $google_api_config['dataset-id'];

try {
	// test the config and connectivity by creating a test entity, building
	// a commit request for that entity, and creating/updating it in the datastore
	$req = create_test_request();
  $service_dataset->commit($dataset_id, $req, []);
}
catch (Google_Exception $ex) {
 syslog(LOG_WARNING, 'Commit to Cloud Datastore exception: ' . $ex->getMessage());
 echo "There was an issue -- check the logs.";
 return;
}

echo "Connected!";
