<?php

namespace Phalcon\Db\Adapter\RethinkDB;

use \r as r;
use Phalcon\Db\Adapter\RethinkDB\Model\DatabaseInfoIterator;
use Phalcon\Db\Adapter\RethinkDB\Operation\DropDatabase;
use Phalcon\Db\Adapter\RethinkDB\Operation\ListDatabases;
use Phalcon\Db\Adapter\RethinkDB\Exception\InvalidArgumentException;

class Client
{
    private $manager;
    private $uri;
    private $typeMap;

    /**
     * Constructs a new Client instance.
     *
     *
     * @param string $host
     * @param string $port
     * @param string $dbname
     *
     */
    public function __construct($host = 'localhost', $port = '28015', $dbname = '')
    {
        $this->manager = r\connect($host, $port, $dbname);
    }

    /**
     * Return internal properties for debugging purposes.
     *
     * @return array
     */
    public function __debugInfo()
    {
        return [
            'manager'=>$this->manager
        ];
    }

    /**
     * Select a database.
     *
     * Note: databases whose names contain special characters (e.g. "-") may
     * be selected with complex syntax (e.g. $client->{"that-database"}) or
     * {@link selectDatabase()}.
     *
     * @see http://php.net/oop5.overloading#object.get
     * @see http://php.net/types.string#language.types.string.parsing.complex
     *
     * @param string $databaseName Name of the database to select
     *
     * @return Database
     */
    public function __get($databaseName)
    {
        return $this->selectDatabase($databaseName);
    }

    /**
     * TODO: delete if not needed
     * Return the connection string (i.e. URI).
     *
     * @return string
     */
    public function __toString()
    {
        return $this->uri;
    }

    /**
     * Drop a database.
     *
     * @see DropDatabase::__construct() for supported options
     *
     * @param string $databaseName Database name
     * @param array  $options Additional options
     *
     * @return object Command result document
     */
    public function dropDatabase($databaseName, array $options = [])
    {
        $operation=new DropDatabase($databaseName, $options);

        return $operation->execute($this->manager);
    }

    /**
     * List databases.
     *
     * @see ListDatabases::__construct() for supported options
     * @return array Command result document
     */
    public function listDatabases(array $options = [])
    {
        $operation=new ListDatabases($options);

        return $operation->execute($this->manager);
    }

    /**
     * Select a table.
     *
     * @see Table::__construct() for supported options
     *
     * @param string $databaseName Name of the database containing the table
     * @param string $tableName Name of the table to select
     * @param array  $options Table constructor options
     *
     * @return Table
     */
    public function selectTable($databaseName, $tableName, array $options = [])
    {
        $options+=['typeMap'=>$this->typeMap];

        return new Table($this->manager, $databaseName, $tableName, $options);
    }

    /**
     * Select a database.
     *
     * @see Database::__construct() for supported options
     *
     * @param string $databaseName Name of the database to select
     * @param array  $options Database constructor options
     *
     * @return Database
     */
    public function selectDatabase($databaseName, array $options = [])
    {
        $options+=['typeMap'=>$this->typeMap];

        return new Database($this->manager, $databaseName, $options);
    }
}
