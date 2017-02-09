<?php

namespace Phalcon\Db\Adapter\RethinkDB\Operation;

use \r as r;
use Phalcon\Db\Adapter\RethinkDB\Exception\InvalidArgumentException;
use Phalcon\Db\Adapter\RethinkDB\Exception\UnexpectedValueException;
use Phalcon\Db\Adapter\RethinkDB\Functions;

/**
 * Operation for the count command.
 *
 * @package Phalcon\Db\Adapter\RethinkDB\Operation
 */
class Count implements Executable
{
    private $tableName;
    private $filter;
    private $options;

    /**
     * Constructs a count command.
     *
     * Supported options:
     *
     *  * hint (string|document): The index to use. If a document, it will be
     *    interpretted as an index specification and a name will be generated.
     *
     *  * limit (integer): The maximum number of documents to count.
     *
     *  * maxTimeMS (integer): The maximum amount of time to allow the query to
     *    run.
     *
     *  * readConcern (RethinkDB\Driver\ReadConcern): Read concern.
     *
     *    For servers < 3.2, this option is ignored as read concern is not
     *    available.
     *
     *  * readPreference (RethinkDB\Driver\ReadPreference): Read preference.
     *
     *  * skip (integer): The number of documents to skip before returning the
     *    documents.
     *
     * @param string       $tableName Table name
     * @param array|object $filter Query by which to filter documents
     * @param array        $options Command options
     *
     * @throws InvalidArgumentException
     */
    public function __construct($tableName, $filter = [], array $options = [])
    {
        if (!is_array($filter)&&!is_object($filter)) {
            throw InvalidArgumentException::invalidType('$filter', $filter, 'array or object');
        }

        if (isset($options['hint'])) {
            if (is_array($options['hint'])||is_object($options['hint'])) {
                $options['hint']=Functions::generateIndexName($options['hint']);
            }

            if (!is_string($options['hint'])) {
                throw InvalidArgumentException::invalidType(
                    '"hint" option',
                    $options['hint'],
                    'string or array or object'
                );
            }
        }

        if (isset($options['limit'])&&!is_integer($options['limit'])) {
            throw InvalidArgumentException::invalidType('"limit" option', $options['limit'], 'integer');
        }

        if (isset($options['skip'])&&!is_integer($options['skip'])) {
            throw InvalidArgumentException::invalidType('"skip" option', $options['skip'], 'integer');
        }

        $this->tableName= (string)$tableName;
        $this->filter        =$filter;
        $this->options       =$options;
    }

    /**
     * Execute the operation.
     *
     * @see Executable::execute()
     *
     * @param r\Connection $manager
     * @return int
     * @throws \Exception
     * @internal param Server $server
     *
     */
    public function execute(r\Connection $manager)
    {
        try {
            $result = r\table($this->tableName)->filter($this->filter)->count()->run($manager);
        } catch (\Exception $e) {
            throw new \Exception("Error: $e");
        }

        return (integer)$result;
    }
}
