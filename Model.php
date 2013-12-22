<?php
/**
 * Model.php - Basic base class for Cloud DataStore Objects.
 */

require_once 'DatastoreService.php';

abstract class Model {

  // See https://developers.google.com/datastore/docs/concepts/#Datastore_Kinds_keys_and_identifiers
  // for information about the key concepts. Either you assign a numberic ID, or
  // a string name, or neither and let the datastore auto assign an ID.
  protected $key_id = null;
  protected $key_name = null;

  protected function __construct() {
  }

  // any subclass of Model should implement these methods
  abstract protected function getKindProperties();
  // abstract protected static function getKindName();
  protected static function getKindName() {
        throw new RuntimeException("Unimplemented");
    }
  // abstract protected static function extractQueryResults($results);
  protected static function extractQueryResults($results) {
        throw new RuntimeException("Unimplemented");
    }

  /**
   * Do a 'put' of the item into the datastore. Will create the key and then call
   * getKindProperties to get the array of properties to write.
   * Optionally indicate the transaction for the operation.
   * @param $txn
   */
    public function put($txn = null) {
    $entity = $this->create_entity();

    $mutation = new Google_Service_Datastore_Mutation();
    if ($this->key_id || $this->key_name) {
      $mutation->setUpsert([$entity]);
    } else {
      $mutation->setInsertAutoId([$entity]);
    }
    $req = new Google_Service_Datastore_CommitRequest();
    if ($txn) {
      syslog(LOG_DEBUG, "doing put in transactional context $txn");
      $req->setTransaction($txn);
    }
    else {
      $req->setMode('NON_TRANSACTIONAL');
    }
    $req->setMutation($mutation);
    DatastoreService::getInstance()->commit($req);
    $this->onItemWrite();

  }

  /**
   * Fetch an object from the datastore by key name.  Optionally indicate transaction.
   * @param $key_name
   * @param $txn
   */
  public static function fetch_by_name($key_name, $txn = null) {
    $path = new Google_Service_Datastore_KeyPathElement();
    $path->setKind(static::getKindName());
    $path->setName($key_name);
    $key = DatastoreService::getInstance()->createKey();
    $key->setPath([$path]);
    return self::fetch_by_key($key, $txn);
  }

  /**
   * Fetch an object from the datastore by key id.  Optionally indicate transaction.
   * @param $key_id
   * @param $txn
   */
  public static function fetch_by_id($key_id, $txn = null) {
    $path = new Google_Service_Datastore_KeyPathElement();
    $path->setKind(static::getKindName());
    $path->setId($key_id);
    $key = DatastoreService::getInstance()->createKey();
    $key->setPath([$path]);
    return self::fetch_by_key($key, $txn);
  }

  /**
   * Fetch an object from the datastore by key.  Optionally indicate transaction.
   * @param $key
   * @param $txn
   */
  private static function fetch_by_key($key, $txn = null) {
    $lookup_req = new Google_Service_Datastore_LookupRequest();
    $lookup_req->setKeys([$key]);
    if ($txn) {
      // syslog(LOG_DEBUG, "fetching in transactional context $txn");
      $ros = new Google_Service_Datastore_ReadOptions();
      $ros->setTransaction($txn);
      $lookup_req->setReadOptions($ros);
    }
    $response = DatastoreService::getInstance()->lookup($lookup_req);
    $found = $response->getFound();
    $extracted = static::extractQueryResults($found);
    return $extracted;
  }

  protected function create_entity() {
    $entity = new Google_Service_Datastore_Entity();
    $entity->setKey(self::createKeyForItem($this));
    $entity->setProperties($this->getKindProperties());
    return $entity;
  }

  /**
   * Delete a value from the datastore.
   * @throws UnexpectedValueException
   */
  public function delete($txn = null) {
    // check for case where ID not defined... don't do rpc call if so.
    if ($this->key_id === null && $this->key_name === null) {
      throw new UnexpectedValueException(
          "Can't delete entity; ID not defined.");
    }
    $this->beforeItemDelete();
    $mutation = new Google_Service_Datastore_Mutation();
    $mutation->setDelete([self::createKeyForItem($this)]);
    $req = new Google_Service_Datastore_CommitRequest();
    if ($txn) {
      syslog(LOG_DEBUG, "doing delete in transactional context $txn");
      $req->setTransaction($txn);
    }
    else {
      $req->setMode('NON_TRANSACTIONAL');
    }
    $req->setMutation($mutation);
    DatastoreService::getInstance()->commit($req);
  }

  /**
   * Query the Datastore for all entities of this Kind
   */
  public static function all() {
    $query = self::createQuery(static::getKindName());
    $results = self::executeQuery($query);
    return static::extractQueryResults($results);
  }

  public static function batchTxnMutate($txn, $batchput, $deletes = []) {
    if (!$txn) {
      throw new UnexpectedValueException('Transaction value not set.');
    }
    $insert_auto_id_items = [];
    $upsert_items = [];
    $delete_items = [];
    foreach ($batchput as $item) {
      $entity = $item->create_entity();
      if($item->key_id || $item->key_name) {
        $upsert_items[] = $entity;
      } else {
        $insert_auto_id_items[] = $entity;
      }
    }
    foreach ($deletes as $delitem) {
      $delitem->beforeItemDelete();
      $delete_items[] = self::createKeyForItem($delitem);
    }
    $mutation = new Google_Service_Datastore_Mutation();
    if (!empty($insert_auto_id_items)) {
      $mutation->setInsertAutoId($insert_auto_id_items);
    }
    if (!empty($upsert_items)) {
      $mutation->setUpsert($upsert_items);
    }
    if (!empty($delete_items)) {
      $mutation->setDelete($delete_items);
    }
    $req = new Google_Service_Datastore_CommitRequest();
    $req->setMutation($mutation);
    $req->setTransaction($txn);
    // will throw Google_Service_Exception if there is contention
    DatastoreService::getInstance()->commit($req);
    // successful commit. Call the onItemWrite method on each of the batch put items
    foreach($batchput as $item) {
      $item->onItemWrite();
    }
  }

  /**
   * Do a non-transactional batch put.  Split into sub-batches
   * if the list is too big.
   */
  public static function putBatch($batchput) {
    $insert_auto_id_items = [];
    $upsert_items = [];
    $batch_limit = 490;
    $count = 0;

    // process the inserts/updates
    foreach ($batchput as $item) {
      $entity = $item->create_entity();

      if($item->key_id || $item->key_name) {
        $upsert_items[] = $entity;
      } else {
        $insert_auto_id_items[] = $entity;
      }
      $count++;
      if ($count > $batch_limit) {
        // we've reached the batch limit-- write what we have so far
        $mutation = new Google_Service_Datastore_Mutation();
        if (!empty($insert_auto_id_items)) {
          $mutation->setInsertAutoId($insert_auto_id_items);
        }
        // TODO -- why was this an 'else'?
        // else if (!empty($upsert_items)) {
        if (!empty($upsert_items)) {
          $mutation->setUpsert($upsert_items);
        }

        $req = new Google_Service_Datastore_CommitRequest();
        $req->setMutation($mutation);
        $req->setMode('NON_TRANSACTIONAL');
        DatastoreService::getInstance()->commit($req);
        // reset the batch count and lists
        $count = 0;
        $insert_auto_id_items = [];
        $upsert_items = [];
      }
    }
    // insert the remainder.
    $mutation = new Google_Service_Datastore_Mutation();
    syslog(LOG_DEBUG, "inserts " . count($insert_auto_id_items) . ", upserts " . count($upsert_items));
    if (!empty($insert_auto_id_items)) {
      $mutation->setInsertAutoId($insert_auto_id_items);
    }
    if (!empty($upsert_items)) {
      $mutation->setUpsert($upsert_items);
    }
    $req = null;
    $req = new Google_Service_Datastore_CommitRequest();
    $req->setMutation($mutation);
    $req->setMode('NON_TRANSACTIONAL');
    DatastoreService::getInstance()->commit($req);

    //now, call the onItemWrite method on each of the batch put items
    foreach($batchput as $item) {
      $item->onItemWrite();
    }
  }

  protected function setKeyId($id) {
    $this->key_id = $id;
  }

  protected function setKeyName($name) {
    $this->key_name = $name;
  }

  /**
   * Can be used by derived classes to update in-memory cache when an item is
   * written.
   */
  protected function onItemWrite() {
  }

  /**
   * Can be used by derived classes to delete from in-memory cache when an item is
   * deleted.
   */
  protected function beforeItemDelete() {
  }

  /**
   * Will create either string or list of strings property,
   * depending upon parameter passed.
   */
  protected function createStringProperty($str, $indexed = false) {
    $prop = new Google_Service_Datastore_Property();
    if (is_string($str)) {
      $prop->setStringValue($str);
      $prop->setIndexed($indexed);
    }
    else {
      if (is_array($str)) {
        $vals = [];
        foreach ($str as $s) {
          $value = new Google_Service_Datastore_Value();
          $value->setStringValue($s);
          $value->setIndexed($indexed);
          $vals[] = $value;
        }
      $prop->setListValue($vals);
    }}
    return $prop;
  }

  /**
   * Create list of string values property.
   */
  protected function createStringListProperty($strlist, $indexed = false) {
    $prop = new Google_Service_Datastore_Property();
    $vals = [];
    foreach ($strlist as $s) {
      $value = new Google_Service_Datastore_Value();
      $value->setStringValue($s);
      $value->setIndexed($indexed);
      $vals[] = $value;
    }
    $prop->setListValue($vals);
    return $prop;
  }

  /**
   * Create date property.
   */
  protected function createDateProperty($date, $indexed = false) {
    $prop = new Google_Service_Datastore_Property();
    $dateValue = new DateTime($date);
    $prop->setDateTimeValue($dateValue->format(DateTime::ATOM));
    $prop->setIndexed($indexed);
    return $prop;
  }

  /**
   * Create a query object for the given Kind.
   */
  protected static function createQuery($kind_name) {
    $query = new Google_Service_Datastore_Query();
    $kind = new Google_Service_Datastore_KindExpression();
    $kind->setName($kind_name);
    $query->setKinds([$kind]);
    return $query;
  }

  /**
   * Execute the given query and return the results.
   */
  protected static function executeQuery($query) {
    $req = new Google_Service_Datastore_RunQueryRequest();
    $req->setQuery($query);
    $response = DatastoreService::getInstance()->runQuery($req);

    if (isset($response['batch']['entityResults'])) {
      return $response['batch']['entityResults'];
    } else {
      return [];
    }
  }

  /**
   * Create a query filter on a string property.
   * @param $name
   * @param $value
   * @param $operator
   */
  protected static function createStringFilter($name, $value, $operator = 'equal') {
    $filter_value = new Google_Service_Datastore_Value();
    $filter_value->setStringValue($value);
    $property_ref = new Google_Service_Datastore_PropertyReference();
    $property_ref->setName($name);
    $property_filter = new Google_Service_Datastore_PropertyFilter();
    $property_filter->setProperty($property_ref);
    $property_filter->setValue($filter_value);
    $property_filter->setOperator($operator);
    $filter = new Google_Service_Datastore_Filter();
    $filter->setPropertyFilter($property_filter);
    return $filter;
  }

  /**
   * Create a query filter on a date property.
   * @param $name property name
   * @param $value property value
   * @param $operator filter operator
   */
  protected static function createDateFilter($name, $value, $operator = 'greaterThan') {
    $date_value = new Google_Service_Datastore_Value();
    $date_time = new DateTime($value);
    $date_value->setDateTimeValue($date_time->format(DateTime::ATOM));
    $property_ref = new Google_Service_Datastore_PropertyReference();
    $property_ref->setName($name);
    $property_filter = new Google_Service_Datastore_PropertyFilter();
    $property_filter->setProperty($property_ref);
    $property_filter->setValue($date_value);
    $property_filter->setOperator($operator);
    $filter = new Google_Service_Datastore_Filter();
    $filter->setPropertyFilter($property_filter);
    return $filter;
  }

  /**
   * Create a property 'order' and add it to a datastore query
   * @param $query the datastore query object
   * @param $name property name
   * @param $direction sort direction
   */
  protected static function addOrder($query, $name, $direction = 'descending') {
    $order = new Google_Service_Datastore_PropertyOrder();
    $property_ref = new Google_Service_Datastore_PropertyReference();
    $property_ref->setName($name);
    $order->setProperty($property_ref);
    $order->setDirection($direction);
    $query->setOrder([$order]);
  }

  /**
   * Create a composite 'and' filter.
   * @param $filters Array of filters
   */
  protected static function createCompositeFilter($filters) {
    $composite_filter = new Google_Service_Datastore_CompositeFilter();
    $composite_filter->setOperator('and');
    $composite_filter->setFilters($filters);
    $filter = new Google_Service_Datastore_Filter();
    $filter->setCompositeFilter($composite_filter);
    return $filter;
  }

  /**
   * Generate the Key for the item.
   * @param $item
   */
  protected static function createKeyForItem($item) {
    $path = new Google_Service_Datastore_KeyPathElement();
    $path->setKind($item->getKindName());
    // Sanity check
    if ($item->key_id !== null && $item->key_name !== null) {
      throw new UnexpectedValueException(
        'Only one of key_id or key_name should be set.');
    }

    if ($item->key_id !== null) {
      $path->setId($item->key_id);
    } else if ($item->key_name !== null) {
      $path->setName($item->key_name);
    }

    $key = DatastoreService::getInstance()->createKey();
    $key->setPath([$path]);
    return $key;
  }
}
