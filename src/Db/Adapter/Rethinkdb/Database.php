<?php

namespace Phalcon\Db\Adapter\RethinkDB;

use \r as r;
use Phalcon\Db\Adapter\RethinkDB\Table;
use Phalcon\Db\Adapter\RethinkDB\Exception\InvalidArgumentException;
use Phalcon\Db\Adapter\RethinkDB\Model\TableInfoIterator;
use Phalcon\Db\Adapter\RethinkDB\Operation\CreateTable;
use Phalcon\Db\Adapter\RethinkDB\Operation\DatabaseCommand;
use Phalcon\Db\Adapter\RethinkDB\Operation\DropTable;
use Phalcon\Db\Adapter\RethinkDB\Operation\DropDatabase;
use Phalcon\Db\Adapter\RethinkDB\Operation\ListTables;

class Database
{
    private $databaseName;
    private $manager;

    /**
     * Constructs new Database instance.
     *
     * This class provides methods for database-specific operations and serves
     * as a gateway for accessing tables.
     *
     * Supported options:
     *
     *  * readConcern (RethinkDB\Driver\ReadConcern): The default read concern to
     *    use for database operations and selected tables. Defaults to the
     *    Manager's read concern.
     *
     *  * readPreference (RethinkDB\Driver\ReadPreference): The default read
     *    preference to use for database operations and selected tables.
     *    Defaults to the Manager's read preference.
     *
     *  * typeMap (array): Default type map for cursors and BSON documents.
     *
     *  * writeConcern (RethinkDB\Driver\WriteConcern): The default write concern
     *    to use for database operations and selected tables. Defaults to
     *    the Manager's write concern.
     *
     * @param Manager $manager Manager instance from the driver
     * @param string  $databaseName Database name
     * @param array   $options Database options
     *
     * @throws InvalidArgumentException
     */
    public function __construct(r\Connection $manager, $databaseName, array $options = [])
    {
        if (strlen($databaseName)<1) {
            throw new InvalidArgumentException('$databaseName is invalid: '.$databaseName);
        }

        $this->manager       =$manager;
        $this->databaseName  =(string)$databaseName;

        $this->manager->useDb($this->databaseName);
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
            'databaseName'  =>$this->databaseName,
            'manager'       =>$this->manager
        ];
    }

    /**
     * Select a table within this database.
     *
     * Note: tables whose names contain special characters (e.g. ".") may
     * be selected with complex syntax (e.g. $database->{"system.profile"}) or
     * {@link selectTable()}.
     *
     * @see http://php.net/oop5.overloading#object.get
     * @see http://php.net/types.string#language.types.string.parsing.complex
     *
     * @param string $tableName Name of the table to select
     *
     * @return Table
     */
    public function __get($tableName)
    {
        return $this->selectTable($tableName);
    }

    /**
     * Return the database name.
     *
     * @return string
     */
    public function __toString()
    {
        return $this->databaseName;
    }

    /**
     * Execute a command on this database.
     *
     * @see DatabaseCommand::__construct() for supported options
     *
     * @param array|object $command Command document
     * @param array        $options Options for command execution
     *
     * @return Cursor
     * @throws InvalidArgumentException
     */
    public function command($command, array $options = [])
    {
        if (!isset($options['readPreference'])) {
            $options['readPreference']=$this->readPreference;
        }

        if (!isset($options['typeMap'])) {
            $options['typeMap']=$this->typeMap;
        }

        $operation=new DatabaseCommand($this->databaseName, $command, $options);
        $server   =$this->manager->selectServer($options['readPreference']);

        return $operation->execute($server);
    }

    /**
     * Create a new table explicitly.
     *
     * @see CreateTable::__construct() for supported options
     *
     * @param string $tableName
     * @param array  $options
     *
     * @return array|object Command result document
     */
    public function createTable($tableName, array $options = [])
    {
        if (!isset($options['typeMap'])) {
            $options['typeMap']=$this->typeMap;
        }

        $operation=new CreateTable($this->databaseName, $tableName, $options);
        $server   =$this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Drop this database.
     *
     * @see DropDatabase::__construct() for supported options
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

        $operation=new DropDatabase($this->databaseName, $options);
        $server   =$this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Drop a table within this database.
     *
     * @see DropTable::__construct() for supported options
     *
     * @param string $tableName Table name
     * @param array  $options Additional options
     *
     * @return array|object Command result document
     */
    public function dropTable($tableName, array $options = [])
    {
        if (!isset($options['typeMap'])) {
            $options['typeMap']=$this->typeMap;
        }

        $operation=new DropTable($this->databaseName, $tableName, $options);
        $server   =$this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Returns the database name.
     *
     * @return string
     */
    public function getDatabaseName()
    {
        return $this->databaseName;
    }

    /**
     * Returns information for all tables in this database.
     *
     * @see ListTables::__construct() for supported options
     *
     * @param array $options
     *
     * @return TableInfoIterator
     */
    public function listTables(array $options = [])
    {
        $operation=new ListTables($this->databaseName, $options);
        $server   =$this->manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        return $operation->execute($server);
    }

    /**
     * Select a table within this database.
     *
     * @see Table::__construct() for supported options
     *
     * @param string $tableName Name of the table to select
     * @param array  $options Table constructor options
     *
     * @return Table
     */
    public function selectTable($tableName, array $options = [])
    {
        return new Table($this->manager, $this->databaseName, $tableName, $options);
    }

    /**
     * Get a clone of this database with different options.
     *
     * @see Database::__construct() for supported options
     *
     * @param array $options Database constructor options
     *
     * @return Database
     */
    public function withOptions(array $options = [])
    {
        $options+=[
            'readConcern'   =>$this->readConcern,
            'readPreference'=>$this->readPreference,
            'typeMap'       =>$this->typeMap,
            'writeConcern'  =>$this->writeConcern,
        ];

        return new Database($this->manager, $this->databaseName, $options);
    }
}
