<?php

/*
  +------------------------------------------------------------------------+
  | Phalcon Framework                                                      |
  +------------------------------------------------------------------------+
  | Copyright (c) 2011-2016 Phalcon Team (https://www.phalconphp.com)      |
  +------------------------------------------------------------------------+
  | This source file is subject to the New BSD License that is bundled     |
  | with this package in the file LICENSE.txt.                             |
  |                                                                        |
  | If you did not receive a copy of the license and are unable to         |
  | obtain it through the world-wide-web, please send an email             |
  | to license@phalconphp.com so we can send you a copy immediately.       |
  +------------------------------------------------------------------------+
  | Authors: Ben Casey <bcasey@tigerstrikemedia.com>                       |
  +------------------------------------------------------------------------+
*/

namespace Phalcon\Db\Adapter\RethinkDB\Operation;

use Phalcon\Db\Adapter\RethinkDB\Functions;
use Phalcon\Db\Adapter\RethinkDB\UpdateResult;
use Phalcon\Db\Adapter\RethinkDB\Exception\InvalidArgumentException;
use \r as r;

/**
 * Operation for updating a single document with the update command.
 *
 * @package Phalcon\Db\Adapter\RethinkDB\Operation
 */
class UpdateOne implements Executable
{
    private $update;

    /**
     * Constructs an update command.
     *
     * Supported options:
     *
     *  * bypassDocumentValidation (boolean): If true, allows the write to opt
     *    out of document level validation.
     *
     *  * upsert (boolean): When true, a new document is created if no document
     *    matches the query. The default is false.
     *
     *  * writeConcern (RethinkDB\Driver\WriteConcern): Write concern.
     *
     * @param string       $databaseName Database name
     * @param string       $tableName Table name
     * @param array|object $filter Query by which to filter documents
     * @param array|object $update Update to apply to the matched document
     * @param array        $options Command options
     *
     * @throws InvalidArgumentException
     */
    public function __construct($tableName, $filter, $update, array $options = [])
    {
        if (!is_array($update)&&!is_object($update)) {
            throw InvalidArgumentException::invalidType('$update', $update, 'array or object');
        }

        if (!Functions::isFirstKeyOperator($update)) {
            throw new InvalidArgumentException('First key in $update argument is not an update operator');
        }

        $this->update=new Update($tableName, $filter, $update, ['multi'=>false]+$options);
    }

    /**
     * Execute the operation.
     *
     * @see Executable::execute()
     *
     * @param r\Connection $manager
     * @return string
     *
     */
    public function execute(r\Connection $manager)
    {
        return $this->update->execute($manager);
    }
}
