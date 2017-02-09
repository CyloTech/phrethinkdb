<?php

namespace Phalcon\Db\Adapter\RethinkDB\Operation;

use Phalcon\Db\Adapter\RethinkDB\Functions;
use Phalcon\Db\Adapter\RethinkDB\Exception\InvalidArgumentException;
use \r as r;

/**
 * Operation for the find command.
 *
 * @package Phalcon\Db\Adapter\RethinkDB\Operation
 */
class Find implements Executable
{
    const NON_TAILABLE=1;
    const TAILABLE=2;
    const TAILABLE_AWAIT=3;
	
    private $tableName;
    private $filter;
    private $options;

    /**
     * Constructs a find command.
     *
     * Supported options:
     *
     *  * limit (integer): The maximum number of documents to return.
     *
     *  * skip (integer): The number of documents to skip before returning.
     *
     *  * orderby (string): The field name to order results by.
     *
     * @param string       $tableName Table name
     * @param array|object $filter Query by which to filter documents
     * @param array        $options Command options
     *
     * @throws InvalidArgumentException
     */
    public function __construct($tableName, $filter, array $options = [])
    {
        if (!is_array($filter)&&!is_object($filter)) {
            throw InvalidArgumentException::invalidType('$filter', $filter, 'array or object');
        }

        if (isset($options['limit'])&&!is_integer($options['limit'])) {
            throw InvalidArgumentException::invalidType('"limit" option', $options['limit'], 'integer');
        }

        if (isset($options['skip'])&&!is_integer($options['skip'])) {
            throw InvalidArgumentException::invalidType('"skip" option', $options['skip'], 'integer');
        }

	    if (isset($options['orderby'])) {
		    throw InvalidArgumentException::invalidType('"orderby" option', $options['orderby'], 'string');
	    }

        $this->tableName = (string)$tableName;
        $this->filter = $filter;
        $this->options = $options;
    }

    /**
     * Execute the operation.
     *
     * @see Executable::execute()
     */
    public function execute(r\Connection $manager)
    {
	    try {
		    $result = r\table($this->tableName);
		    $result->filter($this->filter);

	    	if(isset($this->options['orderby'])){
			    $result->orderBy($this->options['orderby']);
	    	}

	    	if(isset($this->options['limit'])){
	    		$result->limit($this->options['limit']);
		    }

		    if(isset($this->options['skip'])){
	    		$result->skip($this->options['skip']);
		    }

		    $result->run($manager);


	    } catch (\Exception $e) {
		    throw new \Exception("Error: $e");
	    }

	    return $result->id;
    }
}
