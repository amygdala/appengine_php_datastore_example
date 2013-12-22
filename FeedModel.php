<?php

require_once 'Model.php';

/**
 * Model class for feed objects
 */
class FeedModel extends Model {

  const FEED_MODEL_KIND = 'FeedModelTest';
  const SUBSCRIBER_URL_NAME = 'subscriber_url';

  private $subscriber_url;

  public function __construct($url) {
    parent::__construct();
    $this->key_name = sha1($url);
    $this->subscriber_url = $url;
  }

  public function getSubscriberUrl() {
    return $this->subscriber_url;
  }


  protected static function getKindName() {
    return self::FEED_MODEL_KIND;
  }

  /**
   * Generate the entity property map from the feed object fields.
   */
  protected function getKindProperties() {
    $property_map = [];

    $property_map[self::SUBSCRIBER_URL_NAME] =
        parent::createStringProperty($this->subscriber_url, true);
    return $property_map;
  }


  /**
   * Fetch a feed object given its feed URL.  If get a cache miss, fetch from the Datastore.
   * @param $feed_url URL of the feed.
   */
  public static function get($feed_url) {
    $mc = new Memcache();
    $key = self::getCacheKey($feed_url);
    $response = $mc->get($key);
    if ($response) {
      return [$response];
    }

    $query = parent::createQuery(self::FEED_MODEL_KIND);
    $feed_url_filter = parent::createStringFilter(self::SUBSCRIBER_URL_NAME,
        $feed_url);
    $filter = parent::createCompositeFilter([$feed_url_filter]);
    $query->setFilter($filter);
    $results = parent::executeQuery($query);
    $extracted = self::extractQueryResults($results);
    return $extracted;
  }

  /**
   * This method will be called after a Datastore put.
   */
  protected function onItemWrite() {
    $mc = new Memcache();
    try {
      $key = self::getCacheKey($this->subscriber_url);
      $mc->add($key, $this, 0, 120);
    }
    catch (Google_Cache_Exception $ex) {
      syslog(LOG_WARNING, "in onItemWrite: memcache exception");
    }
  }

  /**
  * This method will be called prior to a datastore delete
  */
  protected function beforeItemDelete() {
    $mc = new Memcache();
    $key = self::getCacheKey($this->subscriber_url);
    $mc->delete($key);
  }

  /**
   * Extract the results of a Datastore query into FeedModel objects
   * @param $results Datastore query results
   */
  protected static function extractQueryResults($results) {
    $query_results = [];
    foreach($results as $result) {
      $id = @$result['entity']['key']['path'][0]['id'];
      $key_name = @$result['entity']['key']['path'][0]['name'];
      $props = $result['entity']['properties'];
      $url = $props[self::SUBSCRIBER_URL_NAME]->getStringValue();

      $feed_model = new FeedModel($url);
      $feed_model->setKeyId($id);
      $feed_model->setKeyName($key_name);
      // Cache this read feed.
      $feed_model->onItemWrite();

      $query_results[] = $feed_model;
    }
    return $query_results;
  }

  private static function getCacheKey($feed_url) {
    return sprintf("%s_%s", self::FEED_MODEL_KIND, sha1($feed_url));
  }
}
