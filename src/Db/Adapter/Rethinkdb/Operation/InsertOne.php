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
use Phalcon\Db\Adapter\RethinkDB\Exception\InvalidArgumentException;
use \r as r;

/**
 * Operation for inserting a single document with the insert command.
 *
 * @package Phalcon\Db\Adapter\RethinkDB\Operation
 */
class InsertOne implements Executable
{
    private $tableName;
    private $document;
    private $options;

    /**
     * Constructs an insert command.
     *
     * Supported options:
     *
     *  * bypassDocumentValidation (boolean): If true, allows the write to opt
     *    out of document level validation.
     *
     *  * writeConcern (RethinkDB\Driver\WriteConcern): Write concern.
     *
     * @param string       $databaseName Database name
     * @param string       $tableName Table name
     * @param array|object $document Document to insert
     * @param array        $options Command options
     *
     * @throws InvalidArgumentException
     */
    public function __construct($databaseName, $tableName, $document, array $options = [])
    {
        if (!is_array($document)&&!is_object($document)) {
            throw InvalidArgumentException::invalidType('$document', $document, 'array or object');
        }

        if (isset($options['bypassDocumentValidation'])&&!is_bool($options['bypassDocumentValidation'])) {
            throw InvalidArgumentException::invalidType(
                '"bypassDocumentValidation" option',
                $options['bypassDocumentValidation'],
                'boolean'
            );
        }

        $this->tableName=(string)$tableName;
        $this->document      =$document;
        $this->options       =$options;
    }

    /**
     * Execute the operation.
     *
     * @see Executable::execute()
     *
     * @param r\Connection $manager
     * @return InsertOneResult
     * @throws \Exception
     *
     */
    public function execute(r\Connection $manager)
    {
        $options=[];

        if (isset($this->options['bypassDocumentValidation'])) {
            $options['bypassDocumentValidation']=$this->options['bypassDocumentValidation'];
        }

        try {
            $result = r\table($this->tableName)->insert($this->document, $this->options)->run($manager);
        } catch (\Exception $e) {
            throw new \Exception("Error: $e");
        }

        return $result->id;
    }
}
