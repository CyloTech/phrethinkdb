<?php

namespace Phalcon\Db\Adapter\RethinkDB\Operation;

use \r as r;

/**
 * Operation for the ListDatabases command.
 *
 * @package Phalcon\Db\Adapter\RethinkDB\Operation
 */
class ListDatabases implements Executable
{
    private $options;

    /**
     * Constructs a listDatabases commandd
     *
     * @param array $options Command options
     *
     */
    public function __construct(array $options = [])
    {
        $this->options=$options;
    }

    /**
     * Execute the operation.
     *
     * @see Executable::execute()
     *
     * @param r\Connection $manager
     * @return array
     * @throws \Exception
     * @internal param Server $server
     *
     */
    public function execute(r\Connection $manager)
    {
        try {
            $result = r\dbList()->run($manager);
        } catch (\Exception $e) {
            throw new \Exception("Error: $e");
        }

        return $result;
    }
}
