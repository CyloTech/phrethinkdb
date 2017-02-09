<?php

namespace Phalcon\Db\Adapter\RethinkDB\Operation;

use \r as r;

/**
 * Executable interface for operation classes.
 *
 * This interface is reserved for internal use until PHPC-378 is implemented,
 * since execute() should ultimately be changed to use ServerInterface.
 *
 * @package Phalcon\Db\Adapter\RethinkDB\Operation
 */
interface Executable
{
    /**
     * Execute the operation.
     *
     * @param r\Connection $manager
     * @return mixed
     * @internal r\connect $manager
     */
    public function execute(r\Connection $manager);
}
