<?php
/**
 * Wrapper around the Google_DatastoreService class.
 */

// Assumes v1 of client libs: https://github.com/google/google-api-php-client
// 1.0-alpha paths
require_once 'Google/Client.php';
require_once 'Google/Auth/AssertionCredentials.php';
require_once 'Google/Service/Datastore.php';

final class DatastoreService {

  private static $instance = null;

  private static $required_options = [
    'dataset-id',
    'application-id',
  ];

  static $scopes = [
    "https://www.googleapis.com/auth/datastore",
    "https://www.googleapis.com/auth/userinfo.email",
  ];

  private $dataset;

  private $dataset_id;

  private $config = [
  ];

  /**
   * @return DatastoreService The instance of the service.
   * @throws UnexpectedValueException
   */
  public static function getInstance() {
    if (self::$instance == null) {
      throw new UnexpectedValueException('Instance has not been set.');
    }
    return self::$instance;
  }

  public static function setInstance($instance) {
    if (self::$instance != null) {
      throw new UnexpectedValueException('Instance has already been set.');
    }
    self::$instance = $instance;
  }

  /**
   * @param $options - Array with values to configure the service. Options are:
   *   - client-id
   *   - client-secret
   *   - redirect-url
   *   - developer-key
   *   - application-id
   *   - service-account-name
   *   - private-key
   *   - namespace
   */
  public function __construct($options) {
    $this->config = array_merge($this->config, $options);
    $this->init($this->config);
  }

  // Helper functions for Cloud Datastore services, abstracts the dataset.
  public function allocateIds(Google_Service_Datastore_AllocateIdsRequest $postBody, $optParams = []) {
    return $this->dataset->allocateIds($this->dataset_id, $postBody, $optParams);
  }

  public function beginTransaction(Google_Service_Datastore_BeginTransactionRequest $postBody, $optParams = array()) {
    return $this->dataset->beginTransaction($this->dataset_id, $postBody, $optParams);
  }

  public function commit(Google_Service_Datastore_CommitRequest $postBody, $optParams = []) {
    return $this->dataset->commit($this->dataset_id, $postBody, $optParams);
  }

  public function lookup(Google_Service_Datastore_LookupRequest $postBody, $optParams = []) {
    return $this->dataset->lookup($this->dataset_id, $postBody, $optParams);
  }

  public function rollback(Google_Service_Datastore_RollbackRequest $postBody, $optParams = []) {
    return $this->dataset->rollback($this->dataset_id, $postBody, $optParams);
  }

  public function runQuery(Google_Service_Datastore_RunQueryRequest $postBody, $optParams = []) {
    return $this->dataset->runQuery($this->dataset_id, $postBody, $optParams);
  }

  // Key helper function, abstracts the namespace
  public function createKey() {
    $key = new Google_Service_Datastore_Key();

    if (isset($this->config['namespace'])) {
      $partition = new Google_Service_Datastore_PartitionId();
      $partition->setNamespace($this->config['namespace']);
      $key->setPartitionId($partition);
    }

    return $key;
  }

  private function init($options) {
    foreach(self::$required_options as $required_option) {
      if (!array_key_exists($required_option, $options)) {
        throw new InvalidArgumentException(
          'Option ' . $required_option . ' must be supplied.');
      }
    }
    $client = new Google_Client();
    $client->setApplicationName($options['application-id']);
    // 1.0-alpha version
    $client->setAssertionCredentials(new Google_Auth_AssertionCredentials(
      $options['service-account-name'],
      self::$scopes,
      $options['private-key']));
    $service = new Google_Service_Datastore($client);

    $this->dataset = $service->datasets;
    $this->dataset_id = $options['dataset-id'];
  }
}
