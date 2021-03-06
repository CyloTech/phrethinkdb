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
 * Operation for the update command.
 *
 * This class is used internally by the ReplaceOne, UpdateMany, and UpdateOne
 * operation classes.
 *
 * @package Phalcon\Db\Adapter\RethinkDB\Operation
 */
class Update implements Executable
{
    private $tableName;
    private $filter;
    private $update;
    private $options;

    /**
     * Constructs a update command.
     *
     * Supported options:
     *
     *  * bypassDocumentValidation (boolean): If true, allows the write to opt
     *    out of document level validation.
     *
     *  * multi (boolean): When true, updates all documents matching the query.
     *    This option cannot be true if the $update argument is a replacement
     *    document (i.e. contains no update operators). The default is false.
     *
     *  * upsert (boolean): When true, a new document is created if no document
     *    matches the query. The default is false.
     *
     *  * writeConcern (RethinkDB\Driver\WriteConcern): Write concern.
     *
     * @param string       $databaseName Database name
     * @param string       $tableName Table name
     * @param array|object $filter Query by which to delete documents
     * @param array|object $update Update to apply to the matched
     *                                     document(s) or a replacement document
     * @param array        $options Command options
     *
     * @throws InvalidArgumentException
     */
    public function __construct($tableName, $filter, $update, array $options = [])
    {
        if (!is_array($filter)&&!is_object($filter)) {
            throw InvalidArgumentException::invalidType('$filter', $filter, 'array or object');
        }

        if (!is_array($update)&&!is_object($update)) {
            throw InvalidArgumentException::invalidType('$update', $filter, 'array or object');
        }

        $options+=[
            'multi' =>false,
            'upsert'=>false,
        ];

        if (isset($options['bypassDocumentValidation'])&&!is_bool($options['bypassDocumentValidation'])) {
            throw InvalidArgumentException::invalidType(
                '"bypassDocumentValidation" option',
                $options['bypassDocumentValidation'],
                'boolean'
            );
        }

        if (!is_bool($options['multi'])) {
            throw InvalidArgumentException::invalidType('"multi" option', $options['multi'], 'boolean');
        }

        if ($options['multi']&&!Functions::isFirstKeyOperator($update)) {
            throw new InvalidArgumentException('"multi" option cannot be true if $update is a replacement document');
        }

        if (!is_bool($options['upsert'])) {
            throw InvalidArgumentException::invalidType('"upsert" option', $options['upsert'], 'boolean');
        }

        $this->databaseName  =(string)$databaseName;
        $this->tableName=(string)$tableName;
        $this->filter        =$filter;
        $this->update        =$update;
        $this->options       =$options;
    }

    /**
     * Execute the operation.
     *
     * @see Executable::execute()
     *
     * @param Server $server
     *
     * @return UpdateResult
     */
    public function execute(r\Connection $manager)
    {
        $updateOptions=[
            'multi' =>$this->options['multi'],
            'upsert'=>$this->options['upsert'],
        ];

        $bulkOptions=[];

        if (isset($this->options['bypassDocumentValidation'])&&Functions::serverSupportsFeature(
            $server,
            self::$wireVersionForDocumentLevelValidation
        )
        ) {
            $bulkOptions['bypassDocumentValidation']=$this->options['bypassDocumentValidation'];
        }

        $bulk=new Bulk($bulkOptions);
        $bulk->update($this->filter, $this->update, $updateOptions);

        $writeConcern=isset($this->options['writeConcern'])?$this->options['writeConcern']:null;
        $writeResult =$server->executeBulkWrite($this->databaseName.'.'.$this->tableName, $bulk, $writeConcern);

        return new UpdateResult($writeResult);
    }
}
