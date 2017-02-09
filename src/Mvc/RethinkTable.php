<?php

namespace Phalcon\Mvc;

use Phalcon\Di;
use \r as r;
use Phalcon\Mvc\Collection\Document;
use Phalcon\Mvc\Collection\Exception;
use Phalcon\Db\Adapter\RethinkDB\InsertOneResult;
use Phalcon\Mvc\Collection as PhalconCollection;
use Phalcon\Db\Adapter\RethinkDB\Table as AdapterCollection;

/**
 * Class RethinkTable
 *
 * @property  \Phalcon\Mvc\Collection\ManagerInterface _modelsManager
 * @package Phalcon\Mvc
 */
abstract class RethinkTable extends \Phalcon\Mvc\Collection
{
    // @codingStandardsIgnoreStart
    static protected $_disableEvents;
    // @codingStandardsIgnoreEnd

    /**
     * {@inheritdoc}
     *
     * @param mixed $id
     */
    public function setId($id)
    {
        if (is_object($id)) {
            $this->id = $id;
            return;
        }

        if ($this->_modelsManager->isUsingImplicitObjectIds($this)) {
            $this->id = new ObjectID($id);
            return;
        }

        $this->id = $id;
    }

    /**
     * {@inheritdoc}
     *
     * @return bool
     *
     * @throws Exception
     */
    public function save()
    {

        $dependencyInjector = $this->_dependencyInjector;

        if (!is_object($dependencyInjector)) {
            throw new Exception(
                "A dependency injector container is required to obtain the services related to the ODM"
            );
        }

        $source = $this->getSource();

        if (empty($source)) {
            throw new Exception("Method getSource() returns empty string");
        }

        $connection = $this->getDI()->get('rethink');

        $table = $connection->selectTable($source);

        $exists = $this->_exists($table);

        if (false === $exists) {
            $this->_operationMade = self::OP_CREATE;
        } else {
            $this->_operationMade = self::OP_UPDATE;
        }

        /**
         * The messages added to the validator are reset here
         */
        $this->_errorMessages = [];

        $disableEvents = self::$_disableEvents;

        /**
         * Execute the preSave hook
         */
        if (false === $this->_preSave($dependencyInjector, $disableEvents, $exists)) {
            return false;
        }

        $data = $this->toArray();
die();
        /**
         * We always use safe stores to get the success state
         * Save the document
         */
        switch ($this->_operationMade) {
            case self::OP_CREATE:
                $insertedId = $table->insertOne($data);
                break;

            case self::OP_UPDATE:
                $insertedId = $table->updateOne(['id' => $this->id], ['$set' => $this->toArray()]);
                break;

            default:
                throw new Exception('Invalid operation requested for ' . __METHOD__);
        }

        $success = false;

        if ($insertedId) {
            $success = true;

            if (false === $exists) {
                $this->id = $insertedId;
            }
        }



        /**
         * Call the postSave hooks
         */
        return $this->_postSave($disableEvents, $success, $exists);
    }

    /**
     * {@inheritdoc}
     *
     * @param mixed $id
     *
     * @return array
     */
    public static function findById($id)
    {
        if (!is_object($id)) {
            $classname = get_called_class();
            $collection = new $classname();

            /** @var RethinkdbCollection $collection */
            if ($collection->getCollectionManager()->isUsingImplicitObjectIds($collection)) {
                $mongoId = new ObjectID($id);
            } else {
                $mongoId = $id;
            }
        } else {
            $mongoId = $id;
        }

        return static::findFirst([["id" => $mongoId]]);
    }

    /**
     * {@inheritdoc}
     *
     * @param  array|null $parameters
     * @return array
     */
    public static function findFirst(array $parameters = null)
    {
        $className = get_called_class();

        /** @var RethinkdbCollection $collection */
        $collection = new $className();

        $connection = $collection->getConnection();

        return static::_getResultset($parameters, $collection, $connection, true);
    }

    /**
     * {@inheritdoc}
     *
     * @param  array|null $parameters
     * @return array
     */
    public static function find(array $parameters = null)
    {
        $className = get_called_class();

        /** @var RethinkdbCollection $collection */
        $collection = new $className();

        $connection = $collection->getConnection();

        return static::_getResultset($parameters, $collection, $connection, true);
    }

    /**
     * {@inheritdoc}
     *
     * @param array               $params
     * @param CollectionInterface $collection
     * @param \RethinkdbDb            $connection
     * @param bool                $unique
     *
     * @return array
     * @throws Exception
     * @codingStandardsIgnoreStart
     */
    protected static function _getResultset($params, CollectionInterface $collection, $connection, $unique)
    {
        /**
         * @codingStandardsIgnoreEnd
         * Check if "class" clause was defined
         */
        if (isset($params['class'])) {
            $classname = $params['class'];

            $base = new $classname();

            if (!$base instanceof CollectionInterface || $base instanceof Document) {
                throw new Exception(
                    sprintf(
                        'Object of class "%s" must be an implementation of %s or an instance of %s',
                        get_class($classname),
                        CollectionInterface::class,
                        Document::class
                    )
                );
            }
        } else {
            $base = $collection;
        }

        $source = $collection->getSource();

        if (empty($source)) {
            throw new Exception("Method getSource() returns empty string");
        }

//        /**
//         * @var \Phalcon\Db\Adapter\RethinkDB\Table $rethinkdbTable
//         */
//        $rethinkdbTable = $connection->selectTable($source);
//
//        if (!is_object($rethinkdbTable)) {
//            throw new Exception("Couldn't select mongo collection");
//        }

	    /**
	     * Everything below here appears to be correct and complete.
	     * Just above here to complete.
	     *
	     * $rethinkdbTable needs declaring somehow.
	     */

        $conditions = [];

        if (isset($params[0])||isset($params['conditions'])) {
            $conditions = (isset($params[0]))?$params[0]:$params['conditions'];
        }

        /**
         * Convert the string to an array
         */
        if (!is_array($conditions)) {
            throw new Exception("Find parameters must be an array");
        }

        $options = [];

        /**
         * Check if a "limit" clause was defined
         */
        if (isset($params['limit'])) {
            $limit = $params['limit'];

            $options['limit'] = (int)$limit;

            if ($unique) {
                $options['limit'] = 1;
            }
        }

        /**
         * Check if a "orderby" clause was defined
         */
        if (isset($params['orderby'])) {
	        $orderby = $params["orderby"];

            $options['orderby'] = $orderby;
        }

        /**
         * Check if a "skip" clause was defined
         */
        if (isset($params['skip'])) {
            $skip = $params["skip"];

            $options['skip'] = (int)$skip;
        }

        if (isset($params['fields']) && is_array($params['fields']) && !empty($params['fields'])) {
            $options['projection'] = [];

            foreach ($params['fields'] as $key => $show) {
                $options['projection'][$key] = $show;
            }
        }

        /**
         * Perform the find
         */
	    $tableRows = $rethinkdbTable->find($conditions, $options);

        if (true === $unique) {
            /**
             * Looking for only the first result.
             */
            return current($tableRows->toArray());
        }

        /**
         * Requesting a complete resultset
         */
	    $results = [];

        foreach ($tableRows as $document) {
            /**
             * Assign the values to the base object
             */
            $results[] = $document;
        }

        return $results;
    }

    /**
     * {@inheritdoc}
     *
     * <code>
     *    $robot = Robots::findFirst();
     *    $robot->delete();
     *
     *    foreach (Robots::find() as $robot) {
     *        $robot->delete();
     *    }
     * </code>
     */
    public function delete()
    {
        if (!$id = $this->id) {
            throw new Exception("The document cannot be deleted because it doesn't exist");
        }

        $disableEvents = self::$_disableEvents;

        if (!$disableEvents) {
            if (false === $this->fireEventCancel("beforeDelete")) {
                return false;
            }
        }

        if (true === $this->_skipped) {
            return true;
        }

        $connection = $this->getConnection();

        $source = $this->getSource();
        if (empty($source)) {
            throw new Exception("Method getSource() returns empty string");
        }

        /**
         * Get the Collection
         *
         * @var AdapterCollection $collection
         */
        $collection = $connection->selectCollection($source);

        if (is_object($id)) {
            $mongoId = $id;
        } else {
            if ($this->_modelsManager->isUsingImplicitObjectIds($this)) {
                $mongoId = new ObjectID($id);
            } else {
                $mongoId = $id;
            }
        }

        $success = false;

        /**
         * Remove the instance
         */
        $status = $collection->deleteOne(['id' => $mongoId], ['w' => true]);

        if ($status->isAcknowledged()) {
            $success = true;

            $this->fireEvent("afterDelete");
        }

        return $success;
    }

    /**
     * {@inheritdoc}
     *
     * @param  RethinkTable $table
     * @return boolean
     * @codingStandardsIgnoreStart
     */
    protected function _exists($table)
    {
        // @codingStandardsIgnoreStart
        if (!$id = $this->id) {
            return false;
        }

        /**
         * Perform the count using the function provided by the driver
         */
        return $table->count(['id' => $id])>0;
    }

    /**
     * {@inheritdoc}
     *
     * @param string $eventName
     * @return bool
     */
    public function fireEventCancel($eventName)
    {
        /**
         * Check if there is a method with the same name of the event
         */
        if (method_exists($this, $eventName)) {
            if (false === $this->{$eventName}()) {
                return false;
            }
        }

        /**
         * Send a notification to the events manager
         */
        if (false === $this->_modelsManager->notifyEvent($eventName, $this)) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritdoc}
     *
     * @return bool
     */
    public function create()
    {
        /** @var \Phalcon\Db\Adapter\RethinkDB\Collection $collection */
        $collection = $this->prepareCU();

        /**
         * Check the dirty state of the current operation to update the current operation
         */
        $this->_operationMade = self::OP_CREATE;

        /**
         * The messages added to the validator are reset here
         */
        $this->_errorMessages = [];

        /**
         * Execute the preSave hook
         */
        if ($this->_preSave($this->_dependencyInjector, self::$_disableEvents, false) === false) {
            return false;
        }

        $data = $this->toArray();
        $success = false;

        /**
         * We always use safe stores to get the success state
         * Save the document
         */
        $result = $collection->insert($data, ['writeConcern' => new WriteConcern(1)]);
        if ($result instanceof InsertOneResult && $result->getInsertedId()) {
            $success = true;
            $this->id = $result->getInsertedId();
        }

        /**
         * Call the postSave hooks
         */
        return $this->_postSave(self::$_disableEvents, $success, false);
    }

    /**
     * {@inheritdoc}
     *
     * @param array $data
     */
    public function bsonUnserialize(array $data)
    {
        $this->setDI(Di::getDefault());
        $this->_modelsManager = Di::getDefault()->getShared('collectionManager');

        foreach ($data as $key => $val) {
            $this->{$key} = $val;
        }

        if (method_exists($this, "afterFetch")) {
            $this->afterFetch();
        }
    }
}
