<?php

namespace BaoPham\DynamoDb\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use BadMethodCallException;
use BaoPham\DynamoDb\DynamoDbClientInterface;
use BaoPham\DynamoDb\RawDynamoDbQuery;
use Illuminate\Support\Str;

/**
 * Class QueryBuilder
 *
 * @package BaoPham\DynamoDb\DynamoDb
 *
 * Methods are in the form of `set<key_name>`, where `<key_name>`
 * is the key name of the query body to be sent.
 *
 * For example, to build a query:
 * [
 *     'AttributeDefinitions' => ...,
 *     'GlobalSecondaryIndexUpdates' => ...
 *     'TableName' => ...
 * ]
 *
 * Do:
 *
 * $query = $query->setAttributeDefinitions(...)->setGlobalSecondaryIndexUpdates(...)->setTableName(...);
 *
 * When ready:
 *
 * $query->prepare()->updateTable();
 *
 * Common methods:
 *
 * @method QueryBuilder setExpressionAttributeNames(array $mapping)
 * @method QueryBuilder setExpressionAttributeValues(array $mapping)
 * @method QueryBuilder setFilterExpression(string $expression)
 * @method QueryBuilder setKeyConditionExpression(string $expression)
 * @method QueryBuilder setProjectionExpression(string $expression)
 * @method QueryBuilder setUpdateExpression(string $expression)
 * @method QueryBuilder setAttributeUpdates(array $updates)
 * @method QueryBuilder setConsistentRead(bool $consistent)
 * @method QueryBuilder setScanIndexForward(bool $forward)
 * @method QueryBuilder setExclusiveStartKey(mixed $key)
 * @method QueryBuilder setReturnValues(string $type)
 * @method QueryBuilder setRequestItems(array $items)
 * @method QueryBuilder setTableName(string $table)
 * @method QueryBuilder setIndexName(string $index)
 * @method QueryBuilder setSelect(string $select)
 * @method QueryBuilder setItem(array $item)
 * @method QueryBuilder setKeys(array $keys)
 * @method QueryBuilder setLimit(int $limit)
 * @method QueryBuilder setKey(array $key)
 *
 * @method bool hasKeyConditionExpression()
 */
class QueryBuilder
{
    /**
     * @var DynamoDbClientInterface
     */
    private $service;

    /**
     * Query body to be sent to AWS
     *
     * @var array
     */
    public $query = [];

    public function __construct(DynamoDbClientInterface $service)
    {
        $this->service = $service;
    }

    public function hydrate(array $query)
    {
        $this->query = $query;

        return $this;
    }

    public function setExpressionAttributeName($placeholder, $name)
    {
        $this->query['ExpressionAttributeNames'][$placeholder] = $name;

        return $this;
    }

    public function setExpressionAttributeValue($placeholder, $value)
    {
        $this->query['ExpressionAttributeValues'][$placeholder] = $value;

        return $this;
    }

    /**
     * @param DynamoDbClient|null $client
     *
     * @return ExecutableQuery
     */
    public function prepare(DynamoDbClient $client = null)
    {
        $raw = new RawDynamoDbQuery(null, $this->query);

        return new ExecutableQuery($client ?: $this->service->getClient(), $raw->finalize()->query);
    }

    /**
     * Returns the OP based on the conditions set in the query
     *
     * @return string
     */
    public function getOp()
    {
        if ($this->hasKeyConditionExpression()) {
            return 'Query';
        }

        return 'Scan';
    }

    /**
     * @param  string $method
     * @param  array  $parameters
     *
     * @return mixed
     *
     * @throws BadMethodCallException
     */
    public function __call($method, $parameters)
    {
        if (Str::startsWith($method, 'set')) {
            $key = $this->getKey($method, 'set');

            $this->query[$key] = current($parameters);

            return $this;
        }

        if (Str::startsWith($method, 'has')) {
            $key = $this->getKey($method, 'has');

            return array_key_exists($key, $this->query);
        }

        throw new BadMethodCallException(sprintf(
            'Method %s::%s does not exist.',
            static::class,
            $method
        ));
    }

    /**
     * Gets the key by removing the start from the param
     *
     * @param string $method
     * @param string $start
     *
     * @return string
     */
    protected function getKey($method, $start)
    {
        return array_reverse(explode($start, $method, 2))[0];
    }
}
