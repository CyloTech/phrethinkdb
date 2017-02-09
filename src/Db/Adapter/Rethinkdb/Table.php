<?php

namespace Phalcon\Db\Adapter\RethinkDB;

use \r as r;
use Traversable;
use Phalcon\Db\Adapter\RethinkDB\Model;
use Phalcon\Db\Adapter\RethinkDB\Operation;
use Phalcon\Db\Adapter\RethinkDB\Exception\InvalidArgumentException;

/**
 * Phalcon\Db\Adapter\RethinkDB\Table
 *
 * @package Phalcon\Db\Adapter\RethinkDB
 */
class Table
{
    private $tableName;
    private $databaseName;
    private $manager;

    /**
     * Constructs new Table instance.
     *
     * This class provides methods for table-specific operations, such as
     * CRUD (i.e. create, read, update, and delete) and index management.
     *
     * Supported options:
     *
     *  * readConcern (RethinkDB\Driver\ReadConcern): The default read concern to
     *    use for table operations. Defaults to the Manager's read concern.
     *
     *  * readPreference (RethinkDB\Driver\ReadPreference): The default read
     *    preference to use for table operations. Defaults to the Manager's
     *    read preference.
     *
     *  * typeMap (array): Default type map for cursors and BSON documents.
     *
     *  * writeConcern (RethinkDB\Driver\WriteConcern): The default write concern
     *    to use for table operations. Defaults to the Manager's write
     *    concern.
     *
     * @param r\Connection $manager Manager instance from the driver
     * @param string  $databaseName Database name
     * @param string  $tableName Table name
     * @param array   $options Table options
     *
     * @throws InvalidArgumentException
     */
    public function __construct(r\Connection $manager, $databaseName, $tableName, array $options = [])
    {
        if (strlen($databaseName)<1) {
            throw new InvalidArgumentException('$databaseName is invalid: '.$databaseName);
        }

        if (strlen($tableName)<1) {
            throw new InvalidArgumentException('$tableName is invalid: '.$tableName);
        }

        $this->manager       =$manager;
        $this->databaseName  =(string)$databaseName;
        $this->tableName=(string)$tableName;
    }

    /**
     * Return internal properties for debugging purposes.
     *
     * @see http://php.net/manual/en/language.oop5.magic.php#language.oop5.magic.debuginfo
     * @return array
     */
    public function __debugInfo()
    {
        return [
            'tableName'=>$this->tableName,
            'databaseName'  =>$this->databaseName,
            'manager'       =>$this->manager
        ];
    }

    /**
     * Return the table namespace (e.g. "db.table").
     *
     * @see https://docs.RethinkDB.org/manual/faq/developers/#faq-dev-namespace
     * @return string
     */
    public function __toString()
    {
        return $this->databaseName.'.'.$this->tableName;
    }

    /**
     * Executes multiple write operations.
     *
     * @param array[] $operations List of write operations
     * @param array   $options Command options
     *
     * @return BulkWriteResult
     */
    public function bulkWrite(array $operations, array $options = [])
    {
        $operation=new Operation\BulkWrite($this->databaseName, $this->tableName, $operations, $options);
        $server   =$this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Gets the number of documents matching the filter.
     *
     * @param array|object $filter Query by which to filter documents
     * @param Table $table
     * @param array $options Command options
     * @return int
     */
    public function count(array $filter = [], array $options = [])
    {
        $operation=new Operation\Count($this->tableName, $filter, $options);

        return $operation->execute($this->manager);
    }

    /**
     * Create a single index for the table.
     *
     * @param array|object $key Document containing fields mapped to values,
     *                              which denote order or an index type
     * @param array        $options Index options
     *
     * @return string The name of the created index
     */
    public function createIndex($key, array $options = [])
    {
        return current($this->createIndexes([['key'=>$key]+$options]));
    }

    /**
     * Create one or more indexes for the table.
     *
     * Each element in the $indexes array must have a "key" document, which
     * contains fields mapped to an order or type. Other options may follow.
     * For example:
     *
     *     $indexes = [
     *         // Create a unique index on the "username" field
     *         [ 'key' => [ 'username' => 1 ], 'unique' => true ],
     *         // Create a 2dsphere index on the "loc" field with a custom name
     *         [ 'key' => [ 'loc' => '2dsphere' ], 'name' => 'geo' ],
     *     ];
     *
     * If the "name" option is unspecified, a name will be generated from the
     * "key" document.
     *
     * @param array[] $indexes List of index specifications
     *
     * @return string[] The names of the created indexes
     * @throws InvalidArgumentException if an index specification is invalid
     */
    public function createIndexes(array $indexes)
    {
        $operation=new Operation\CreateIndexes($this->databaseName, $this->tableName, $indexes);
        $server   =$this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Deletes all documents matching the filter.
     *
     * @param array|object $filter Query by which to delete documents
     * @param array        $options Command options
     *
     * @return DeleteResult
     */
    public function deleteMany($filter, array $options = [])
    {
        if (!isset($options['writeConcern'])) {
            $options['writeConcern']=$this->writeConcern;
        }

        $operation=new Operation\DeleteMany($this->databaseName, $this->tableName, $filter, $options);
        $server   =$this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Deletes at most one document matching the filter.
     *
     * @param array|object $filter Query by which to delete documents
     * @param array        $options Command options
     *
     * @return DeleteResult
     */
    public function deleteOne($filter, array $options = [])
    {
        if (!isset($options['writeConcern'])) {
            $options['writeConcern']=$this->writeConcern;
        }

        $operation=new Operation\DeleteOne($this->databaseName, $this->tableName, $filter, $options);
        $server   =$this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Finds the distinct values for a specified field across the table.
     *
     * @param string       $fieldName Field for which to return distinct values
     * @param array|object $filter Query by which to filter documents
     * @param array        $options Command options
     *
     * @return mixed[]
     */
    public function distinct($fieldName, $filter = [], array $options = [])
    {
        if (!isset($options['readConcern'])) {
            $options['readConcern']=$this->readConcern;
        }

        if (!isset($options['readPreference'])) {
            $options['readPreference']=$this->readPreference;
        }

        $operation=new Operation\Distinct($this->databaseName, $this->tableName, $fieldName, $filter, $options);
        $server   =$this->manager->selectServer($options['readPreference']);

        return $operation->execute($server);
    }

    /**
     * Drop this table.
     *
     * @param array $options Additional options
     *
     * @return array|object Command result document
     */
    public function drop(array $options = [])
    {
        if (!isset($options['typeMap'])) {
            $options['typeMap']=$this->typeMap;
        }

        $operation=new Operation\DropTable($this->databaseName, $this->tableName, $options);
        $server   =$this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Drop a single index in the table.
     *
     * @param string $indexName Index name
     * @param array  $options Additional options
     *
     * @return array|object Command result document
     * @throws InvalidArgumentException if $indexName is an empty string or "*"
     */
    public function dropIndex($indexName, array $options = [])
    {
        $indexName=(string)$indexName;

        if ($indexName==='*') {
            throw new InvalidArgumentException('dropIndexes() must be used to drop multiple indexes');
        }

        if (!isset($options['typeMap'])) {
            $options['typeMap']=$this->typeMap;
        }

        $operation=new Operation\DropIndexes($this->databaseName, $this->tableName, $indexName, $options);
        $server   =$this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Drop all indexes in the table.
     *
     * @param array $options Additional options
     *
     * @return array|object Command result document
     */
    public function dropIndexes(array $options = [])
    {
        if (!isset($options['typeMap'])) {
            $options['typeMap']=$this->typeMap;
        }

        $operation=new Operation\DropIndexes($this->databaseName, $this->tableName, '*', $options);
        $server   =$this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Finds documents matching the query.
     *
     * @param array|object $filter Query by which to filter documents
     * @param array        $options Additional options
     *
     * @return Cursor
     */
    public function find($filter = [], array $options = [])
    {
        if (!isset($options['readConcern'])) {
            $options['readConcern']=$this->readConcern;
        }

        if (!isset($options['readPreference'])) {
            $options['readPreference']=$this->readPreference;
        }

        if (!isset($options['typeMap'])) {
            $options['typeMap']=$this->typeMap;
        }

        $operation=new Operation\Find($this->databaseName, $this->tableName, $filter, $options);
        $server   =$this->manager->selectServer($options['readPreference']);

        return $operation->execute($server);
    }

    /**
     * Finds a single document matching the query.
     *
     * @param array|object $filter Query by which to filter documents
     * @param array        $options Additional options
     *
     * @return array|object|null
     */
    public function findOne($filter = [], array $options = [])
    {
        if (!isset($options['readConcern'])) {
            $options['readConcern']=$this->readConcern;
        }

        if (!isset($options['readPreference'])) {
            $options['readPreference']=$this->readPreference;
        }

        if (!isset($options['typeMap'])) {
            $options['typeMap']=$this->typeMap;
        }

        $operation=new Operation\FindOne($this->databaseName, $this->tableName, $filter, $options);
        $server   =$this->manager->selectServer($options['readPreference']);

        return $operation->execute($server);
    }

    /**
     * Finds a single document and deletes it, returning the original.
     *
     * The document to return may be null if no document matched the filter.
     *
     * Note: BSON deserialization of the returned document does not yet support
     * a custom type map (depends on: https://jira.RethinkDB.org/browse/PHPC-314).
     *
     * @param  array|object $filter  Query by which to filter documents
     * @param  array        $options Command options [Optional]
     * @return object|null
     */
    public function findOneAndDelete($filter, array $options = [])
    {
        $server = $this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        if (!isset($options['writeConcern']) && Functions::serverSupportsFeature(
            $server,
            self::$wireVersionForFindAndModifyWriteConcern
        )
        ) {
            $options['writeConcern']=$this->writeConcern;
        }

        $operation = new Operation\FindOneAndDelete($this->databaseName, $this->tableName, $filter, $options);

        return $operation->execute($server);
    }

    /**
     * Finds a single document and replaces it, returning either the original or
     * the replaced document.
     *
     * The document to return may be null if no document matched the filter. By
     * default, the original document is returned. Specify
     * FindOneAndReplace::RETURN_DOCUMENT_AFTER for the "returnDocument" option
     * to return the updated document.
     *
     * Note: BSON deserialization of the returned document does not yet support
     * a custom type map (depends on: https://jira.RethinkDB.org/browse/PHPC-314).
     *
     * @param  array|object $filter      Query by which to filter documents
     * @param  array|object $replacement Replacement document
     * @param  array        $options     Command options [Optional]
     * @return object|null
     */
    public function findOneAndReplace($filter, $replacement, array $options = [])
    {
        $server = $this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        if (!isset($options['writeConcern']) && Functions::serverSupportsFeature(
            $server,
            self::$wireVersionForFindAndModifyWriteConcern
        )
        ) {
            $options['writeConcern'] = $this->writeConcern;
        }

        $operation = new Operation\FindOneAndReplace(
            $this->databaseName,
            $this->tableName,
            $filter,
            $replacement,
            $options
        );

        return $operation->execute($server);
    }

    /**
     * Finds a single document and updates it, returning either the original or
     * the updated document.
     *
     * The document to return may be null if no document matched the filter. By
     * default, the original document is returned. Specify
     * FindOneAndUpdate::RETURN_DOCUMENT_AFTER for the "returnDocument" option
     * to return the updated document.
     *
     * Note: BSON deserialization of the returned document does not yet support
     * a custom type map (depends on: https://jira.RethinkDB.org/browse/PHPC-314).
     *
     * @param  array|object $filter  Query by which to filter documents
     * @param  array|object $update  Update to apply to the matched document
     * @param  array        $options Command options [Optional]
     * @return object|null
     */
    public function findOneAndUpdate($filter, $update, array $options = [])
    {
        $server = $this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        if (!isset($options['writeConcern']) && Functions::serverSupportsFeature(
            $server,
            self::$wireVersionForFindAndModifyWriteConcern
        )
        ) {
            $options['writeConcern'] = $this->writeConcern;
        }

        $operation = new Operation\FindOneAndUpdate(
            $this->databaseName,
            $this->tableName,
            $filter,
            $update,
            $options
        );

        return $operation->execute($server);
    }

    /**
     * Return the table name.
     *
     * @return string
     */
    public function getTableName()
    {
        return $this->tableName;
    }

    /**
     * Return the database name.
     *
     * @return string
     */
    public function getDatabaseName()
    {
        return $this->databaseName;
    }

    /**
     * Return the table namespace.
     *
     * @return string
     */
    public function getNamespace()
    {
        return $this->databaseName . '.' . $this->tableName;
    }

    /**
     * Inserts multiple documents.
     *
     * @param  array[]|object[] $documents The documents to insert
     * @param  array            $options   Command options [Optional]
     * @return InsertManyResult
     */
    public function insertMany(array $documents, array $options = [])
    {
        if (!isset($options['writeConcern'])) {
            $options['writeConcern'] = $this->writeConcern;
        }

        $operation = new Operation\InsertMany($this->databaseName, $this->tableName, $documents, $options);
        $server    = $this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Inserts one document.
     *
     * @param array|object $document The document to insert
     * @param array        $options  Command options [Optional]
     * @return InsertOneResult
     */
    public function insertOne($document, array $options = [])
    {
        if (!isset($options['writeConcern'])) {
            $options['writeConcern'] = $this->writeConcern;
        }

        $operation = new Operation\InsertOne($this->databaseName, $this->tableName, $document, $options);
        $server    = $this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Inserts the document.
     *
     * @param  array|object $document The document to insert
     * @param  array        $options  Command options [Optional]
     * @return mixed
     */
    public function insert($document, array $options = [])
    {
        return $this->insertOne($document, $options);
    }

    /**
     * Returns information for all indexes for the table.
     *
     * @param  array $options Command options [Optional]
     * @return Model\IndexInfoIterator
     */
    public function listIndexes(array $options = [])
    {
        $operation = new Operation\ListIndexes($this->databaseName, $this->tableName, $options);
        $server    = $this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Replaces at most one document matching the filter.
     *
     * @param  array|object $filter      Query by which to filter documents
     * @param  array|object $replacement Replacement document
     * @param  array        $options     Command options [Optional]
     * @return UpdateResult
     */
    public function replaceOne($filter, $replacement, array $options = [])
    {
        if (!isset($options['writeConcern'])) {
            $options['writeConcern'] = $this->writeConcern;
        }

        $operation = new Operation\ReplaceOne(
            $this->databaseName,
            $this->tableName,
            $filter,
            $replacement,
            $options
        );

        $server = $this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Updates all documents matching the filter.
     *
     * @param  array|object $filter  Query by which to filter documents
     * @param  array|object $update  Update to apply to the matched documents
     * @param  array        $options Command options [Optional]
     * @return UpdateResult
     */
    public function updateMany($filter, $update, array $options = [])
    {
        if (!isset($options['writeConcern'])) {
            $options['writeConcern'] = $this->writeConcern;
        }

        $operation = new Operation\UpdateMany($this->databaseName, $this->tableName, $filter, $update, $options);
        $server    = $this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Updates at most one document matching the filter.
     *
     * @param  array|object $filter  Query by which to filter documents
     * @param  array|object $update  Update to apply to the matched document
     * @param  array        $options Command options [Optional]
     * @return UpdateResult
     */
    public function updateOne($filter, $update, array $options = [])
    {
        if (!isset($options['writeConcern'])) {
            $options['writeConcern'] = $this->writeConcern;
        }

        $operation = new Operation\UpdateOne($this->databaseName, $this->tableName, $filter, $update, $options);
        $server    = $this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Get a clone of this table with different options.
     *
     * @param  array $options Table constructor options [Optional]
     * @return Table
     */
    public function withOptions(array $options = [])
    {
        $options += [
            'readConcern'    => $this->readConcern,
            'readPreference' => $this->readPreference,
            'typeMap'        => $this->typeMap,
            'writeConcern'   => $this->writeConcern,
        ];

        return new Table($this->manager, $this->databaseName, $this->tableName, $options);
    }
}
