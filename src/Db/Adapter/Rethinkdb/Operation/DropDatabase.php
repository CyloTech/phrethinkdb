<?php

namespace Phalcon\Db\Adapter\RethinkDB\Operation;

use \r as r;
use Phalcon\Db\Adapter\MongoDB\Exception\InvalidArgumentException;

/**
 * Operation for the dropDatabase command.
 *
 * @package Phalcon\Db\Adapter\RethinkDB\Operation
 */
class DropDatabase implements Executable
{
    private $databaseName;
    private $options;

    /**
     * Constructs a dropDatabase command.
     *
     * Supported options:
     *
     *
     * @param string $databaseName Database name
     * @param array  $options Command options
     */
    public function __construct($databaseName, array $options = [])
    {
        $this->databaseName=(string)$databaseName;
        $this->options     =$options;
    }

    /**
     * Execute the operation.
     *
     * @see Executable::execute()
     *
     * @param r\Connection $manager
     * @return array|object Command result document
     * @throws \Exception
     */
    public function execute(r\Connection $manager)
    {
        try {
            $result = r\dbDrop($this->databaseName)->run($manager);
        } catch (\Exception $e) {
            throw new \Exception("Error: $e");
        }

        return $result;
    }
}
